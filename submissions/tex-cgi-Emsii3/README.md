# tex-cgi-Emsii3: the whole API in pdfTeX, one `pdftex` process per request

The feed API from `SPEC.md`, except the application logic is written in TeX. Request parsing, routing, JWT
verification (SHA-256, HMAC and base64url included, as pure TeX macros), JSON parsing of the POST body, JSON
building, timestamp formatting, validation and error mapping all live in `tex/*.tex`. Think CGI, but the
CGI script is a typesetting engine and it starts fresh for every single request.

This is a joke with a working test suite (`test/run.sh`: `passed=42 failed=0`, golden files byte for byte). It
is **not** going to hold a million users. See "Measured" for how far it does get.

Shoutout to @IdkWhatToWriteHereOhManItWasAlreadyUsed and the CMake submission
([#17](https://github.com/arjaythedev/million-users-challenge/pull/17)), which proved that a build system makes a
perfectly fine web framework. This one is directly inspired by it: same CGI-per-request idea, a different tool
that was never meant for this.

## Updates

Each version is measured the same way (see "Measured": `ubuntu:24.04` container, 10 cores, `ab -k`, 8 s per run,
concurrency 32, per-request directories on `/dev/shm`). Requests per second, p95 in brackets:

| Version | Change | `GET /posts/1` | `GET /feed` | `POST /posts/2/like` |
|---|---|---|---|---|
| v1 | `pdftex` per request; TeX runs `sh` + a libpq CLI (`pgq`) through `-shell-escape`, a new Postgres connection per request | 529 (94 ms) | 437 (113 ms) | 213 (240 ms) |
| v2 | pooled persistent libpq connections in the host; TeX talks to them through a FIFO pair (`db.req` / `db.resp`); no shell, no `pgq`, no `-shell-escape` | 1013 (28 ms) | 851 (31 ms) | 200 (173 ms) |

## Stack

| Piece | Version |
|---|---|
| TeX engine | pdfTeX from Ubuntu's `texlive-binaries` + `texlive-base` (dev machine: pdfTeX 3.141592653-2.6-1.40.29, TeX Live 2026), run in e-TeX mode |
| Host | C99/POSIX, one `poll()` loop; built with `gcc -O2`, linked against libpq |
| DB driver | libpq from Ubuntu's `libpq-dev`, a pool of persistent connections inside the host |
| Postgres server | 18 (separate box) |
| Lua / Python / openssl / any language runtime | none |

No Lua (`luatex` is not installed), no `openssl`, and TeX never shells out for crypto. TeX executes nothing: no
shell, no `-shell-escape`; its only I/O is plain file reads and writes in the request directory.

## Design

Two pieces, each with a strictly limited job:

```
client --HTTP--> bin/host (C + libpq) --fork/exec--> pdftex -fmt=feed '\feedmain'
                  sockets, framing,   <--- db.req / db.resp (two FIFOs in the request dir) --->
                  Postgres pool           all the logic, JWT, JSON, routing
```

**`host/host.c`: sockets, HTTP framing and the database pool.** Listens on `PORT` and `EXTRA_PORT`, keep-alive
(idle timeout 120 s, the spec asks for 75 s or more), reads the request head and a `Content-Length` body
(max 16 KB), and answers in order on each connection. For each complete request it makes a fresh directory
(`mkdtemp` in `$TMPDIR`, else `/dev/shm`, else `/tmp`) and writes three files (plus the two FIFOs described below):

- `meta.txt`: the current time, the server start time and `JWT_SECRET`
- `req.txt`: the request line and headers
- `body.txt`: the body bytes as received

Then it forks `pdftex` with that directory as cwd, waits for it, reads `resp.txt` (status code on line 1, the
JSON body on line 2), relays it with `Content-Length` and removes the directory. If `pdftex` fails or
writes nothing usable, the client gets `500 {"error":"internal server error"}`. The host does not know what a
route, a token or a post is; it only moves database command lines and their answers.

**`tex/feed.tex` + `tex/sha256.tex`: the application.** They are compiled once, at build time, into a
dumped format (`bin/feed.fmt`, ~210 KB) so a request does not re-read or re-tokenize any source: pdfTeX loads
the format and runs `\feedmain`, which reads the three files, routes, checks the token, talks to the database,
and writes `resp.txt`. Things that had to be done by hand in macros: SHA-256 on 16-bit halves (TeX integers
are signed 31-bit), HMAC, base64url decode/encode, a JSON string decoder (`\uXXXX` and surrogate pairs
included), JSON escaping, UTF-8 character counting for the 500-character limit, and Postgres timestamp to
`...T...Z` truncated to milliseconds. Ids and counts are carried as digit strings and never put through
`\numexpr`, so they can exceed 2^31.

**The database: a pool in the host, reached through two FIFOs.** The host keeps `DB_POOL` persistent libpq
connections, driven asynchronously from the same `poll()` loop (`PQconnectStart`/`PQconnectPoll`, non-blocking
sends, `PQconsumeInput`/`PQgetResult`), with the five SPEC statements prepared once per connection and the
session set to `TimeZone=UTC`, `DateStyle=ISO`. One query is in flight per connection; further commands wait in a
FIFO queue. For every request the host `mkfifo`s `db.req` and `db.resp` in the request directory and holds
them open before it forks `pdftex`:

1. If the route needs the database, TeX writes one line to `db.req` with `\immediate\openout`/`\write`/`\closeout`:
   `health`, `feed`, `post <id>`, `create <uid> newbody.txt` or `like <uid> <postid>`. (The body of a new post is
   the file `newbody.txt` in the request directory, one trailing newline stripped.) Routes that need no
   database (404, 400, 401...) never write anything.
2. The host reads the line, queues it, runs the matching statement on the next free connection, and writes
   the answer into `db.resp`, handling partial writes with `POLLOUT` (a feed is ~20 KB, more than a small pipe
   buffer), then closes its end.
3. TeX `\openin`s `db.resp` and `\readline`s until end of file. The lines are the same as before:
   `OK`, `P|id|created_at|hex(author)|likes|hex(body)`, `C|id|created_at`, `L|1`/`L|0`, and
   `ERR|<sqlstate>|<hex message>` (strings are hex, so no escaping problem can reach TeX).

The host opens `db.req` read-write (so it is also a writer and never sees a hangup) and `db.resp` with a
reader it keeps until the child exits (so an answer written before TeX has opened the FIFO is not lost, and
TeX still gets end of file when the host's writer closes). If a connection breaks the host reconnects with
backoff (100 ms doubling to 2 s); a command that finds no usable connection, or waits more than 10 s for one,
gets `ERR|CONN|...`, which is what makes `/health` answer 503 while Postgres is down and 200 again as soon as it
is back, without a restart. If the `pdftex` child dies while its query is running, the result is discarded when
it arrives and the connection goes back to the pool clean.

## Rules

- **SQL is SPEC.md's, verbatim, one statement per request.** The post `SELECT` (with `ORDER BY ... LIMIT 20`
  for the feed, or `WHERE p.id = $1`), `INSERT INTO posts ... RETURNING id, created_at`, and
  `INSERT INTO likes ... ON CONFLICT DO NOTHING`; parameters go through prepared statements (`PQsendQueryPrepared`). `/health` runs `SELECT 1`.
- **No caching of any kind.** TeX is a new process with an empty heap for every request, and the host keeps
  no application data: it relays rows and nothing else. Every request reads Postgres.
- **The JWT is fully verified on every request**: three parts, header with `"alg":"HS256"`, HMAC-SHA256
  signature recomputed in TeX and compared, `exp` against the current time, `sub` a positive integer string,
  `username` a string. Auth is checked before the post id on `POST /posts/:id/like`, as in the spec.
- **Connections:** the host holds `DB_POOL` persistent Postgres connections (default min(2 x cores, 64),
  clamped to 1..512) and shares them between requests, which rule 7 allows. A request still runs exactly one
  SPEC statement; the pool only removes the per-request connect (SCRAM and a backend fork), not the query.
  Concurrent `pdftex` children are still bounded by `MAX_CHILDREN` (default 4 x cores); more database commands
  than connections queue in the host.
- Both ports are served by the same process. No compression (the spec says it is optional).

## Settings

| Env | Default | |
|---|---|---|
| `MAX_CHILDREN` | 4 x online cores | maximum concurrent `pdftex` processes (clamped to 512) |
| `DB_POOL` | min(2 x online cores, 64) | persistent Postgres connections (clamped to 1..512) |
| `TMPDIR` | `/dev/shm` if writable, else `/tmp` | where the per-request directories go |

## Running

```bash
sudo ./install.sh   # gcc, libpq-dev, texlive-binaries, texlive-base (apt)
./build.sh          # bin/host and the format bin/feed.fmt
DATABASE_URL=postgres://user:pass@host:5432/feed JWT_SECRET=million-users-challenge \
  HOST=0.0.0.0 PORT=80 EXTRA_PORT=81 ./start.sh
```

## Measured (not on the benchmark boxes)

An `ubuntu:24.04` container (Docker Desktop, 10 cores, so `MAX_CHILDREN` = 40 and `DB_POOL` = 20) with the
packages from `install.sh`, Postgres 18 in a second container (`max_connections=600`), `ab -k` in the same
container as the server, a fresh copy of the seed database, 8 s per run, per-request directories on tmpfs
(`/dev/shm`, the default), no non-2xx responses (`/like` returns 201 for the first like and 200 after that).
Requests per second, with p95 latency in brackets. "Before" is the previous design (a `sh` + `pgq` process and a
new Postgres connection per request); "now" is the pooled one described above:

| Concurrency | Endpoint | before | now |
|---|---|---|---|
| 1 | `GET /feed` | 80 req/s (13 ms) | 159 req/s (7 ms) |
| 1 | `GET /posts/1` | 99 req/s (11 ms) | 202 req/s (5 ms) |
| 1 | `POST /posts/2/like` (authed) | 42 req/s (25 ms) | 52 req/s (21 ms) |
| 8 | `GET /feed` | 401 req/s (25 ms) | 685 req/s (13 ms) |
| 8 | `GET /posts/1` | 466 req/s (21 ms) | 1050 req/s (10 ms) |
| 8 | `POST /posts/2/like` (authed) | 198 req/s (46 ms) | 239 req/s (39 ms) |
| 32 | `GET /feed` | 437 req/s (113 ms) | 837 req/s (35 ms) |
| 32 | `GET /posts/1` | 529 req/s (94 ms) | 938 req/s (30 ms) |
| 32 | `POST /posts/2/like` (authed) | 213 req/s (240 ms) | 207 req/s (184 ms) |

`DB_POOL` at concurrency 32 (feed / post / like, req/s): 8 gives 651 / 863 / 207, 20 (the default here) gives
837 / 938 / 207, 40 gives 786 / 960 / 204, 64 gives 821 / 984 / 200. Anything from about 16 up is within noise;
a pool below the number of busy cores starts to cost reads. Likes do not move with any of it: they wait for the
commit of a single Postgres, which is the ceiling there.

So about 5 to 7 ms per read request when idle (process start, format load, HMAC in macros, one query over an
already open connection) and a ceiling around 840 to 1000 req/s for reads on 10 cores once they are all busy
spawning `pdftex`. With the per-request directories on disk (`TMPDIR=/var/tmp/...`, overlayfs) instead of
tmpfs, a second run gave the same picture within noise (c=32: feed 811, post 1006, like 234 req/s, against 851,
1013 and 200 for tmpfs in the same run): the files are tiny and the FIFOs never touch the disk anyway.

A million users at one page per 24 s want about 42,000 req/s, roughly 50 times what this does. Expect it to
hold on the order of 20,000 users, and that is a guess from the numbers above, not a measurement. The
16-core number on the real box will be somewhat higher and the 4-core number lower.

There is no cheap fix inside this design: the point of the design is the process per request. Moving the work
out of TeX would make it fast and defeat the submission.

The same setup also passes the test suite (`passed=42 failed=0`; pdfTeX 1.40.25, TeX Live 2023/Debian), as
does macOS with TeX Live 2026. On macOS the process per request is 5 to 6 times slower (about 15 req/s at
concurrency 1, 80 to 90 req/s at most on the same 10 cores; measured with the previous design), because `fork`+`exec` costs much more there.
