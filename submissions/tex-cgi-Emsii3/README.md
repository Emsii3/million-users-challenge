# tex-cgi-Emsii3: the whole API in pdfTeX, one `pdftex` process per request

The feed API from `SPEC.md`, except the application logic is written in TeX. Request parsing, routing, JWT
verification (SHA-256, HMAC and base64url included, as pure TeX macros), JSON parsing of the POST body, JSON
building, timestamp formatting, validation and error mapping all live in `tex/*.tex`. Think CGI, but the
CGI script is a typesetting engine and it starts fresh for every single request.

This is a joke with a working test suite (`test/run.sh`: `passed=42 failed=0`, golden files byte for byte). It
is **not** going to hold a million users. See "Measured" for how far it does get.

## Stack

| Piece | Version |
|---|---|
| TeX engine | pdfTeX from Ubuntu's `texlive-binaries` + `texlive-base` (dev machine: pdfTeX 3.141592653-2.6-1.40.29, TeX Live 2026), run in e-TeX mode |
| Host | C99/POSIX, one `poll()` loop, no libraries; built with `gcc -O2` |
| DB driver | libpq from Ubuntu's `libpq-dev`, in a small C command-line tool (`pgq`) |
| Postgres server | 18 (separate box) |
| Lua / Python / openssl / any language runtime | none |

No Lua (`luatex` is not installed), no `openssl`, and TeX never shells out for crypto. The only things TeX
executes are the database tool and its own file I/O.

## Design

Three pieces, each with a strictly limited job:

```
client --HTTP--> bin/host (C) --fork/exec--> pdftex -fmt=feed '\feedmain' --pipe--> bin/pgq (C + libpq) --> Postgres
                 sockets, framing            all the logic, JWT, JSON, routing        one SPEC statement
```

**`host/host.c`: sockets and HTTP framing, nothing else.** Listens on `PORT` and `EXTRA_PORT`, keep-alive
(idle timeout 120 s, the spec asks for 75 s or more), reads the request head and a `Content-Length` body
(max 16 KB), and answers in order on each connection. For each complete request it makes a fresh directory
(`mkdtemp` in `$TMPDIR`, else `/dev/shm`, else `/tmp`) and writes three files:

- `meta.txt`: the current time, the server start time, `JWT_SECRET` and the path of `bin/pgq`
- `req.txt`: the request line and headers
- `body.txt`: the body bytes as received

Then it forks `pdftex` with that directory as cwd, waits for it, reads `resp.txt` (status code on line 1, the
JSON body on line 2), relays it with `Content-Length` and removes the directory. If `pdftex` fails or
writes nothing usable, the client gets `500 {"error":"internal server error"}`. The host does not know what a
route, a token or a post is.

**`tex/feed.tex` + `tex/sha256.tex`: the application.** They are compiled once, at build time, into a
dumped format (`bin/feed.fmt`, ~210 KB) so a request does not re-read or re-tokenize any source: pdfTeX loads
the format and runs `\feedmain`, which reads the three files, routes, checks the token, talks to the database,
and writes `resp.txt`. Things that had to be done by hand in macros: SHA-256 on 16-bit halves (TeX integers
are signed 31-bit), HMAC, base64url decode/encode, a JSON string decoder (`\uXXXX` and surrogate pairs
included), JSON escaping, UTF-8 character counting for the 500-character limit, and Postgres timestamp to
`...T...Z` truncated to milliseconds. Ids and counts are carried as digit strings and never put through
`\numexpr`, so they can exceed 2^31.

**`pgq/pgq.c`: the database, one statement at a time.** A libpq command-line tool. TeX runs it as
`\openin\fd=|"pgq feed"` (a pipe read, which needs `-shell-escape`) and reads the answer with `\readline`.
It connects with `DATABASE_URL`, runs exactly one statement through `PQexecParams`, prints rows as `|`
separated lines (strings as hex, so no escaping problem can reach TeX), and exits 0 even on a database error
(it prints `ERR|<sqlstate>|<hex message>` and TeX maps it). Commands: `health`, `feed`, `post <id>`,
`create <uid> <bodyfile>`, `like <uid> <postid>`.

## Rules

- **SQL is SPEC.md's, verbatim, one statement per request.** The post `SELECT` (with `ORDER BY ... LIMIT 20`
  for the feed, or `WHERE p.id = $1`), `INSERT INTO posts ... RETURNING id, created_at`, and
  `INSERT INTO likes ... ON CONFLICT DO NOTHING`; parameters go through `PQexecParams`. `/health` runs `SELECT 1`.
- **No caching of any kind.** There is nowhere to cache: every request is a new process with a new empty
  heap. Every request reads Postgres.
- **The JWT is fully verified on every request**: three parts, header with `"alg":"HS256"`, HMAC-SHA256
  signature recomputed in TeX and compared, `exp` against the current time, `sub` a positive integer string,
  `username` a string. Auth is checked before the post id on `POST /posts/:id/like`, as in the spec.
- **Connections:** `pgq` opens one Postgres connection per request and closes it when the request ends. The
  number open at once is bounded by the number of concurrent `pdftex` children, `MAX_CHILDREN` (default
  4 x cores; the rest of the requests wait in a queue in the host). `MAX_CHILDREN` is clamped to 512, and
  the default stays below that on any box with fewer than 128 cores.
- Both ports are served by the same process. No compression (the spec says it is optional).

## Settings

| Env | Default | |
|---|---|---|
| `MAX_CHILDREN` | 4 x online cores | maximum concurrent `pdftex` processes (= Postgres connections); keep it at or below 512 |
| `TMPDIR` | `/dev/shm` if writable, else `/tmp` | where the per-request directories go |

## Running

```bash
sudo ./install.sh   # gcc, libpq-dev, texlive-binaries, texlive-base (apt)
./build.sh          # bin/host, bin/pgq, and the format bin/feed.fmt
DATABASE_URL=postgres://user:pass@host:5432/feed JWT_SECRET=million-users-challenge \
  HOST=0.0.0.0 PORT=80 EXTRA_PORT=81 ./start.sh
```

## Measured (not on the benchmark boxes)

A MacBook with 10 cores (so `MAX_CHILDREN` = 40), Postgres 18 on the same machine, `hey` on the same machine,
fresh copy of the seed database, 8 s per run, all responses 200 (`/like` returns 200 for an already-liked post
and 201 for the first like). Requests per second, with p95 latency in brackets:

| Concurrency | `GET /feed` | `GET /posts/1` | `POST /posts/2/like` (authed) |
|---|---|---|---|
| 1 | 15 req/s (82 ms) | 16 req/s (77 ms) | 14 req/s (78 ms) |
| 8 | 79 req/s (117 ms) | 75 req/s (152 ms) | 72 req/s (126 ms) |
| 40 | 88 req/s (578 ms) | 93 req/s (567 ms) | 78 req/s (653 ms) |

So about 65 to 70 ms per request when idle (process start, format load, HMAC in macros, a new
Postgres connection, query, exit), and a ceiling around 80 to 90 req/s on 10 cores once they are all busy
spawning `pdftex`. A million users at one page per 24 s want about 42,000 req/s, which is roughly 500 times
what this does. Expect it to hold on the order of 2,000 users before p95 passes 500 ms, and that is a guess
from the numbers above, not a measurement. The 16-core number on the real box will be somewhat higher and
the 4-core number lower, and all of them will be small.

The cost is spread over process creation, the format load and the Postgres connection setup, and there is
no cheap fix inside this design: the point of the design is the process per request. Moving the work out of
TeX would make it fast and defeat the submission.

On Linux the numbers should differ (`fork`+`exec` is cheaper, `/dev/shm` is real memory, and a loopback
Postgres connection is cheaper to open). The test suite also passes (`passed=42 failed=0`) in an
`ubuntu:24.04` container with the packages from `install.sh` (pdfTeX 1.40.25, TeX Live 2023/Debian) against
a `postgres:18` container, the same setup as CI.
