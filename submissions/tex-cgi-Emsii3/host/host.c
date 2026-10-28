/* host: a tiny HTTP/1.1 front for the TeX CGI handler (see PLAN Contract 1).
 * One poll() loop, two listening ports, keep-alive connections. Each complete request is written to a fresh temp
 * dir and handled by one `pdftex -fmt=feed '\feedmain'` process (at most MAX_CHILDREN at once, the rest queue).
 * The host knows no application logic: it frames HTTP, runs TeX, and relays resp.txt.
 * It also owns the database: a pool of persistent libpq connections driven from the same poll() loop. TeX talks to
 * it through two FIFOs in its request dir: it writes one command line to db.req and reads the answer from db.resp. */
#define _GNU_SOURCE /* memmem, strcasestr on glibc */
#include <arpa/inet.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <stdarg.h>
#include <stdint.h>
#include <libpq-fe.h>
#ifdef __APPLE__
#include <mach-o/dyld.h>
#endif

#define MAX_BODY 16384
#define MAX_HEAD 16384
#define IDLE_SECS 120
#define CHILD_SECS 30
#define DB_QUEUE_SECS 10
#define DB_CONNECT_MS 5000
#define DB_QUERY_MS 45000
#define BODY500 "{\"error\":\"internal server error\"}"

enum state { READING, QUEUED, RUNNING, WRITING };
enum dbst { DB_NONE, DB_WAITLINE, DB_QUEUED, DB_INFLIGHT, DB_RESP, DB_DONE };

struct dbc;

typedef struct {
  int fd;
  enum state st;
  char *in; size_t inlen, incap;   /* unparsed input */
  size_t reqlen, hdrlen, clen;     /* current complete request: total bytes, header bytes (incl. CRLFCRLF), body bytes */
  int close_after;
  unsigned long seq;               /* FIFO ticket while QUEUED */
  pid_t pid; char dir[PATH_MAX]; time_t started;
  char *out; size_t outlen, outpos;
  time_t last;
  int dead;
  /* database side of the running child */
  enum dbst dbst;
  int reqfd, respfd, keepfd;       /* db.req (O_RDWR), db.resp writer, db.resp keeper reader */
  char dbline[1024]; size_t dblen;
  unsigned long dbseq; time_t dbqt;
  struct dbc *dbc;
  char *dout; size_t dlen, dpos;
} conn;

static conn **conns; static size_t nconns, capconns;
static int running, max_children, selfpipe[2];
static unsigned long seqno;
static time_t start_time;
static char tmpbase[PATH_MAX], fmtpath[PATH_MAX + 16];

static void die(const char *m) { perror(m); exit(1); }

static void on_chld(int s) { (void)s; int e = errno; char c = 0; if (write(selfpipe[1], &c, 1) < 0) {} errno = e; }

static void set_nb_cloexec(int fd) {
  fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK);
  fcntl(fd, F_SETFD, FD_CLOEXEC);
}

/* ---------- paths ---------- */

static void find_paths(const char *argv0) {
  char exe[PATH_MAX] = "";
#ifdef __APPLE__
  uint32_t n = sizeof exe; char tmp[PATH_MAX];
  if (_NSGetExecutablePath(tmp, &n) == 0) realpath(tmp, exe);
#else
  ssize_t n = readlink("/proc/self/exe", exe, sizeof exe - 1);
  exe[n > 0 ? n : 0] = 0;
#endif
  if (!exe[0] && !realpath(argv0, exe)) die("cannot locate executable");
  char *slash = strrchr(exe, '/'); if (slash) *slash = 0;
  snprintf(fmtpath, sizeof fmtpath, "%s/feed", exe);
  const char *t = getenv("TMPDIR");
  if (t && *t) snprintf(tmpbase, sizeof tmpbase, "%s", t);
  else if (access("/dev/shm", W_OK) == 0) strcpy(tmpbase, "/dev/shm");
  else strcpy(tmpbase, "/tmp");
  size_t l = strlen(tmpbase); while (l > 1 && tmpbase[l - 1] == '/') tmpbase[--l] = 0;
}

/* ---------- connections ---------- */

static void close_conn(conn *c) {
  if (c->fd >= 0) close(c->fd);
  c->fd = -1; c->dead = 1;
}

static void compact(void) {
  size_t j = 0;
  for (size_t i = 0; i < nconns; i++) {
    if (conns[i]->dead) { free(conns[i]->in); free(conns[i]->out); free(conns[i]); }
    else conns[j++] = conns[i];
  }
  nconns = j;
}

static void accept_on(int lfd) {
  for (;;) {
    int fd = accept(lfd, NULL, NULL);
    if (fd < 0) return; /* EAGAIN, or transient (EMFILE...): try again next round */
    set_nb_cloexec(fd);
    int one = 1; setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    conn *c = calloc(1, sizeof *c);
    c->reqfd = c->respfd = c->keepfd = -1;
    c->fd = fd; c->st = READING; c->last = time(NULL);
    if (nconns == capconns) conns = realloc(conns, (capconns = capconns ? capconns * 2 : 256) * sizeof *conns);
    conns[nconns++] = c;
  }
}

static void try_requests(conn *c);

static const char *reason(int code) {
  switch (code) {
    case 200: return "OK"; case 201: return "Created"; case 400: return "Bad Request";
    case 401: return "Unauthorized"; case 404: return "Not Found"; case 411: return "Length Required";
    case 413: return "Payload Too Large"; case 431: return "Request Header Fields Too Large";
    case 503: return "Service Unavailable";
    default: return code >= 500 ? "Internal Server Error" : code >= 400 ? "Bad Request" : "OK";
  }
}

/* Writes as much of c->out as the socket takes. Returns 1 when everything is sent (conn is then ready for the next request). */
static void flush_out(conn *c) {
  while (c->outpos < c->outlen) {
    ssize_t n = send(c->fd, c->out + c->outpos, c->outlen - c->outpos, 0);
    if (n > 0) c->outpos += n;
    else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return;
    else if (n < 0 && errno == EINTR) continue;
    else { close_conn(c); return; }
  }
  free(c->out); c->out = NULL;
  if (c->close_after) { close_conn(c); return; }
  /* drop the request we just answered, keep pipelined bytes */
  memmove(c->in, c->in + c->reqlen, c->inlen - c->reqlen);
  c->inlen -= c->reqlen; c->reqlen = 0;
  c->st = READING; c->last = time(NULL);
  try_requests(c);
}

static void respond(conn *c, int code, const char *body, size_t blen) {
  char head[256];
  int hl = snprintf(head, sizeof head,
    "HTTP/1.1 %d %s\r\nContent-Type: application/json\r\nContent-Length: %zu\r\nConnection: %s\r\n\r\n",
    code, reason(code), blen, c->close_after ? "close" : "keep-alive");
  c->out = malloc(hl + blen); memcpy(c->out, head, hl); memcpy(c->out + hl, body, blen);
  c->outlen = hl + blen; c->outpos = 0; c->st = WRITING; c->last = time(NULL);
  flush_out(c);
}

static void respond_err(conn *c, int code, const char *msg) {
  char b[128]; int n = snprintf(b, sizeof b, "{\"error\":\"%s\"}", msg);
  c->close_after = 1; respond(c, code, b, n);
}

/* Looks at the buffered bytes; if one full request is present, marks the conn QUEUED for the dispatcher. */
static void try_requests(conn *c) {
  if (c->st != READING || c->dead || c->inlen == 0) return;
  char *he = memmem(c->in, c->inlen, "\r\n\r\n", 4);
  if (!he) {
    if (c->inlen > MAX_HEAD) respond_err(c, 431, "headers too large");
    return;
  }
  size_t hdrlen = he - c->in + 4, clen = 0; int ka = 1, have_te = 0;
  if (hdrlen > MAX_HEAD) { respond_err(c, 431, "headers too large"); return; }
  char *p = memchr(c->in, '\n', hdrlen);               /* skip request line */
  if (!p) { respond_err(c, 400, "bad request"); return; }
  if (memmem(c->in, p - c->in, "HTTP/1.0", 8)) ka = 0;
  for (p++; p < c->in + hdrlen - 2;) {
    char *eol = memchr(p, '\n', c->in + hdrlen - p);
    if (!eol) break;
    if (!strncasecmp(p, "content-length:", 15)) clen = strtoul(p + 15, NULL, 10);
    else if (!strncasecmp(p, "transfer-encoding:", 18)) have_te = 1;
    else if (!strncasecmp(p, "connection:", 11)) {
      char line[128]; size_t l = eol - p < 127 ? eol - p : 127; memcpy(line, p, l); line[l] = 0;
      if (strcasestr(line, "close")) ka = 0; else if (strcasestr(line, "keep-alive")) ka = 1;
    }
    p = eol + 1;
  }
  if (have_te) { respond_err(c, 411, "chunked bodies are not supported"); return; }
  if (clen > MAX_BODY) { respond_err(c, 413, "body too large"); return; }
  if (c->inlen < hdrlen + clen) return;                 /* wait for the rest of the body */
  c->hdrlen = hdrlen; c->clen = clen; c->reqlen = hdrlen + clen; c->close_after = !ka;
  c->st = QUEUED; c->seq = ++seqno;
}

static void on_readable(conn *c) {
  if (c->inlen + 4096 > c->incap) c->in = realloc(c->in, c->incap = (c->incap ? c->incap * 2 : 4096) + 4096);
  ssize_t n = recv(c->fd, c->in + c->inlen, c->incap - c->inlen, 0);
  if (n > 0) { c->inlen += n; c->last = time(NULL); try_requests(c); }
  else if (n == 0 || (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)) close_conn(c);
}

/* ---------- database pool ---------- */

#define POST_SELECT \
  "SELECT p.id, p.body, p.created_at, u.username,\n" \
  "       (SELECT count(*) FROM likes l WHERE l.post_id = p.id)::int AS like_count\n" \
  "  FROM posts p JOIN users u ON u.id = p.user_id"

enum { S_HEALTH, S_FEED, S_POST, S_CREATE, S_LIKE, NSTMT };
static const char *stmt_sql[NSTMT] = {
  "SELECT 1",
  POST_SELECT "\n ORDER BY p.created_at DESC, p.id DESC LIMIT 20",
  POST_SELECT "\n WHERE p.id = $1",
  "INSERT INTO posts (user_id, body) VALUES ($1, $2) RETURNING id, created_at",
  "INSERT INTO likes (user_id, post_id) VALUES ($1, $2) ON CONFLICT (user_id, post_id) DO NOTHING",
};

enum dstate { D_DOWN, D_CONNECTING, D_PREPARING, D_IDLE, D_BUSY };
typedef struct dbc {
  PGconn *pg; enum dstate st;
  short polldir; int wantout;
  int prep, stmt;
  int64_t retry_at, since, backoff;
  conn *owner; PGresult *res;
} dbc;

static dbc *pool; static int npool;
static const char *dburl;
static char last_dberr[256] = "database not connected";
static unsigned long dbseqno;

static int64_t now_ms(void) {
  struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
  return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

typedef struct { char *p; size_t n, cap; } sbuf;
static void sb_put(sbuf *b, const char *s, size_t n) {
  if (b->n + n + 1 > b->cap) { b->cap = (b->n + n + 1) * 2; b->p = realloc(b->p, b->cap); }
  memcpy(b->p + b->n, s, n); b->n += n; b->p[b->n] = 0;
}
static void sb_str(sbuf *b, const char *s) { sb_put(b, s, strlen(s)); }
static void sb_hex(sbuf *b, const char *s, size_t n) {
  static const char hx[] = "0123456789abcdef";
  for (size_t i = 0; i < n; i++) { char t[2] = { hx[(unsigned char)s[i] >> 4], hx[(unsigned char)s[i] & 15] }; sb_put(b, t, 2); }
}
static void sb_err(sbuf *b, const char *state, const char *msg) {
  sb_str(b, "ERR|"); sb_str(b, state && *state ? state : "CONN"); sb_str(b, "|");
  if (msg) {
    size_t n = strlen(msg); /* libpq messages end with a newline: drop it */
    while (n && (msg[n - 1] == '\n' || msg[n - 1] == '\r')) n--;
    sb_hex(b, msg, n);
  }
  sb_str(b, "\n");
}

static void db_try_write(conn *c);

/* Takes ownership of the buffer and starts pushing it into db.resp. */
static void db_reply(conn *c, sbuf *b) {
  c->dout = b->p; c->dlen = b->n; c->dpos = 0; c->dbst = DB_RESP;
  db_try_write(c);
}
static void db_reply_err(conn *c, const char *state, const char *msg) {
  sbuf b = {0}; sb_err(&b, state, msg); db_reply(c, &b);
}

static void db_try_write(conn *c) {
  while (c->dpos < c->dlen) {
    ssize_t n = write(c->respfd, c->dout + c->dpos, c->dlen - c->dpos);
    if (n > 0) c->dpos += n;
    else if (n < 0 && errno == EINTR) continue;
    else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return;
    else break; /* cannot happen while the keeper fd is open; give up, TeX sees EOF */
  }
  free(c->dout); c->dout = NULL;
  close(c->respfd); c->respfd = -1; /* the only writer: TeX now reads EOF (the keeper keeps unread data alive) */
  c->dbst = DB_DONE;
}

static void db_release(conn *c) {
  if (c->dbc) c->dbc->owner = NULL; /* result is discarded when it arrives */
  c->dbc = NULL;
  if (c->reqfd >= 0) close(c->reqfd);
  if (c->respfd >= 0) close(c->respfd);
  if (c->keepfd >= 0) close(c->keepfd);
  c->reqfd = c->respfd = c->keepfd = -1;
  free(c->dout); c->dout = NULL; c->dbst = DB_NONE;
}

static void db_format(conn *c, dbc *d) {
  PGresult *r = d->res; sbuf b = {0};
  ExecStatusType st = r ? PQresultStatus(r) : PGRES_FATAL_ERROR;
  if (!r) sb_err(&b, "CONN", PQerrorMessage(d->pg));
  else if (d->stmt == S_HEALTH && st == PGRES_TUPLES_OK) sb_str(&b, "OK\n");
  else if ((d->stmt == S_FEED || d->stmt == S_POST) && st == PGRES_TUPLES_OK) {
    for (int i = 0; i < PQntuples(r); i++) {
      sb_str(&b, "P|"); sb_str(&b, PQgetvalue(r, i, 0)); sb_str(&b, "|"); sb_str(&b, PQgetvalue(r, i, 2)); sb_str(&b, "|");
      sb_hex(&b, PQgetvalue(r, i, 3), strlen(PQgetvalue(r, i, 3))); sb_str(&b, "|"); sb_str(&b, PQgetvalue(r, i, 4)); sb_str(&b, "|");
      sb_hex(&b, PQgetvalue(r, i, 1), strlen(PQgetvalue(r, i, 1))); sb_str(&b, "\n");
    }
  } else if (d->stmt == S_CREATE && st == PGRES_TUPLES_OK && PQntuples(r) == 1) {
    sb_str(&b, "C|"); sb_str(&b, PQgetvalue(r, 0, 0)); sb_str(&b, "|"); sb_str(&b, PQgetvalue(r, 0, 1)); sb_str(&b, "\n");
  } else if (d->stmt == S_LIKE && st == PGRES_COMMAND_OK) {
    sb_str(&b, strcmp(PQcmdTuples(r), "0") ? "L|1\n" : "L|0\n");
  } else {
    const char *sq = PQresultErrorField(r, PG_DIAG_SQLSTATE);
    sb_err(&b, sq, PQresultErrorMessage(r));
  }
  db_reply(c, &b);
}

static void dbc_close_pg(dbc *d) { if (d->pg) PQfinish(d->pg); d->pg = NULL; }

/* The connection is unusable: answer its owner (if still around), reconnect later with backoff. */
static void dbc_fail(dbc *d, const char *why) {
  const char *m = d->pg && PQerrorMessage(d->pg) && *PQerrorMessage(d->pg) ? PQerrorMessage(d->pg) : why;
  snprintf(last_dberr, sizeof last_dberr, "%s", m);
  size_t l = strlen(last_dberr); while (l && (last_dberr[l - 1] == '\n' || last_dberr[l - 1] == '\r')) last_dberr[--l] = 0;
  if (d->res) { PQclear(d->res); d->res = NULL; }
  if (d->owner) { conn *o = d->owner; d->owner = NULL; o->dbc = NULL; db_reply_err(o, "CONN", last_dberr); }
  dbc_close_pg(d);
  d->st = D_DOWN; d->backoff = d->backoff ? (d->backoff * 2 > 2000 ? 2000 : d->backoff * 2) : 100;
  d->retry_at = now_ms() + d->backoff;
}

static void dbc_connect(dbc *d) {
  const char *keys[] = {"dbname", "options", NULL};
  const char *vals[] = {dburl && *dburl ? dburl : NULL, "-c TimeZone=UTC -c DateStyle=ISO", NULL};
  d->pg = PQconnectStartParams(keys, vals, 1);
  d->since = now_ms();
  if (!d->pg || PQstatus(d->pg) == CONNECTION_BAD) { dbc_fail(d, "cannot start connection"); return; }
  d->st = D_CONNECTING; d->polldir = POLLOUT; d->wantout = 0;
}

static int dbc_flush(dbc *d) {
  int f = PQflush(d->pg);
  if (f < 0) { dbc_fail(d, "connection lost"); return -1; }
  d->wantout = f;
  return 0;
}

static void dbc_prepare_next(dbc *d) {
  if (d->prep >= NSTMT) { d->st = D_IDLE; d->backoff = 0; return; }
  char name[16]; snprintf(name, sizeof name, "s%d", d->prep);
  if (!PQsendPrepare(d->pg, name, stmt_sql[d->prep], 0, NULL)) { dbc_fail(d, "prepare failed"); return; }
  d->st = D_PREPARING; dbc_flush(d);
}

/* Starts the command line of c on idle connection d. */
static void dbc_run(dbc *d, conn *c) {
  char line[sizeof c->dbline]; memcpy(line, c->dbline, sizeof line);
  char *t[5] = {0}; int nt = 0, bad = 0; char *sp = NULL;
  for (char *w = strtok_r(line, " \t\r\n", &sp); w && nt < 5; w = strtok_r(NULL, " \t\r\n", &sp)) t[nt++] = w;
  const char *vals[2] = {0}; int np = 0, stmt = -1; char *body = NULL;
  if (nt == 1 && !strcmp(t[0], "health")) stmt = S_HEALTH;
  else if (nt == 1 && !strcmp(t[0], "feed")) stmt = S_FEED;
  else if (nt == 2 && !strcmp(t[0], "post")) { stmt = S_POST; vals[np++] = t[1]; }
  else if (nt == 3 && !strcmp(t[0], "like")) { stmt = S_LIKE; vals[np++] = t[1]; vals[np++] = t[2]; }
  else if (nt == 3 && !strcmp(t[0], "create") && !strchr(t[2], '/')) {
    char path[PATH_MAX + 300]; snprintf(path, sizeof path, "%s/%s", c->dir, t[2]);
    FILE *f = fopen(path, "rb");
    if (!f) { db_reply_err(c, "FILE", "cannot read body file"); return; }
    size_t cap = 4096, n = 0, k; body = malloc(cap + 1);
    while ((k = fread(body + n, 1, cap - n, f)) > 0) { n += k; if (n == cap) body = realloc(body, (cap *= 2) + 1); }
    fclose(f);
    if (n && body[n - 1] == '\n') n--; /* strip ONE trailing newline */
    body[n] = 0;
    stmt = S_CREATE; vals[np++] = t[1]; vals[np++] = body;
  } else bad = 1;
  if (bad) { db_reply_err(c, "USAGE", "unknown command"); return; }
  char name[16]; snprintf(name, sizeof name, "s%d", stmt);
  int ok = PQsendQueryPrepared(d->pg, name, np, vals, NULL, NULL, 0);
  free(body);
  d->stmt = stmt; d->st = D_BUSY; d->owner = c; d->since = now_ms(); c->dbc = d; c->dbst = DB_INFLIGHT;
  if (!ok) { dbc_fail(d, "send failed"); return; }
  dbc_flush(d);
}

/* Reads whatever arrived. Handles the PREPARING and BUSY states. */
static void dbc_input(dbc *d) {
  if (!PQconsumeInput(d->pg)) { dbc_fail(d, "connection lost"); return; }
  while (d->pg && !PQisBusy(d->pg)) {
    PGresult *r = PQgetResult(d->pg);
    if (r) { if (d->res) PQclear(r); else d->res = r; continue; }
    if (d->st == D_PREPARING) {
      int good = d->res && PQresultStatus(d->res) == PGRES_COMMAND_OK;
      if (!good) {
        snprintf(last_dberr, sizeof last_dberr, "%s", d->res ? PQresultErrorMessage(d->res) : "prepare failed");
        PQclear(d->res); d->res = NULL; dbc_fail(d, last_dberr); return;
      }
      PQclear(d->res); d->res = NULL; d->prep++; dbc_prepare_next(d); return;
    }
    /* D_BUSY: the query is complete */
    conn *o = d->owner; d->owner = NULL;
    if (o) { o->dbc = NULL; db_format(o, d); }
    if (d->res) { PQclear(d->res); d->res = NULL; }
    if (PQstatus(d->pg) != CONNECTION_OK) { dbc_fail(d, "connection lost"); return; }
    d->st = D_IDLE;
    return;
  }
}

static void dbc_event(dbc *d, short rev) {
  if (d->st == D_CONNECTING) {
    PostgresPollingStatusType ps = PQconnectPoll(d->pg);
    if (ps == PGRES_POLLING_READING) d->polldir = POLLIN;
    else if (ps == PGRES_POLLING_WRITING) d->polldir = POLLOUT;
    else if (ps == PGRES_POLLING_OK) {
      if (PQsetnonblocking(d->pg, 1) != 0) { dbc_fail(d, "cannot set nonblocking"); return; }
      d->prep = 0; dbc_prepare_next(d);
    } else dbc_fail(d, "connection failed");
    return;
  }
  if ((rev & POLLOUT) && d->wantout && dbc_flush(d) < 0) return;
  if (d->st == D_IDLE) {
    if (!PQconsumeInput(d->pg) || PQstatus(d->pg) != CONNECTION_OK) dbc_fail(d, "connection lost");
    else if (rev & (POLLHUP | POLLERR)) dbc_fail(d, "connection lost");
    return;
  }
  if (rev & (POLLIN | POLLHUP | POLLERR)) dbc_input(d);
}

static void db_init(void) {
  long nc = sysconf(_SC_NPROCESSORS_ONLN); if (nc < 1) nc = 1;
  long n = 2 * nc < 64 ? 2 * nc : 64;
  if (getenv("DB_POOL") && atol(getenv("DB_POOL")) > 0) n = atol(getenv("DB_POOL"));
  if (n > 512) n = 512;
  npool = (int)n; dburl = getenv("DATABASE_URL");
  pool = calloc(npool, sizeof *pool);
  for (int i = 0; i < npool; i++) dbc_connect(&pool[i]);
}

/* Hands queued commands to idle connections (oldest first); fails them when the pool is down or they waited too long. */
static void db_dispatch(void) {
  int64_t now = now_ms();
  for (int i = 0; i < npool; i++)
    if (pool[i].st == D_DOWN && now >= pool[i].retry_at) dbc_connect(&pool[i]);
  for (int i = 0; i < npool; i++) {
    dbc *d = &pool[i];
    if (d->st == D_CONNECTING && now - d->since > DB_CONNECT_MS) dbc_fail(d, "connection timeout");
    else if (d->st == D_BUSY && now - d->since > DB_QUERY_MS) dbc_fail(d, "query timeout");
  }
  int live = 0; for (int i = 0; i < npool; i++) if (pool[i].st != D_DOWN) live++;
  time_t tnow = time(NULL);
  for (;;) {
    conn *best = NULL;
    for (size_t i = 0; i < nconns; i++)
      if (conns[i]->dbst == DB_QUEUED && (!best || conns[i]->dbseq < best->dbseq)) best = conns[i];
    if (!best) return;
    if (!live) { db_reply_err(best, "CONN", last_dberr); continue; }
    if (tnow - best->dbqt > DB_QUEUE_SECS) { db_reply_err(best, "CONN", "timed out waiting for a database connection"); continue; }
    dbc *idle = NULL;
    for (int i = 0; i < npool; i++) if (pool[i].st == D_IDLE) { idle = &pool[i]; break; }
    if (!idle) return;
    dbc_run(idle, best);
  }
}

static void db_read_line(conn *c) {
  for (;;) {
    ssize_t n = read(c->reqfd, c->dbline + c->dblen, sizeof c->dbline - 1 - c->dblen);
    if (n > 0) {
      c->dblen += n;
      char *nl = memchr(c->dbline, '\n', c->dblen);
      if (nl) { *nl = 0; c->dbst = DB_QUEUED; c->dbseq = ++dbseqno; c->dbqt = time(NULL); return; }
      if (c->dblen >= sizeof c->dbline - 1) { db_reply_err(c, "USAGE", "command too long"); return; }
    } else if (n < 0 && errno == EINTR) continue;
    else return; /* EAGAIN (or nothing more) */
  }
}

/* ---------- running TeX ---------- */

static int write_file(const char *dir, const char *name, const char *data, size_t len) {
  char path[PATH_MAX + 16]; snprintf(path, sizeof path, "%s/%s", dir, name);
  int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
  if (fd < 0) return -1;
  size_t off = 0;
  while (off < len) { ssize_t n = write(fd, data + off, len - off); if (n <= 0) { close(fd); return -1; } off += n; }
  return close(fd);
}

static void rm_rf(const char *dir) {
  DIR *d = opendir(dir);
  if (d) {
    struct dirent *e; char path[PATH_MAX + 300];
    while ((e = readdir(d))) {
      if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, "..")) continue;
      snprintf(path, sizeof path, "%s/%s", dir, e->d_name);
      struct stat sb;
      if (lstat(path, &sb) == 0 && S_ISDIR(sb.st_mode)) rm_rf(path); else unlink(path);
    }
    closedir(d);
  }
  rmdir(dir);
}

static void spawn(conn *c) {
  snprintf(c->dir, sizeof c->dir, "%s/texcgi.XXXXXX", tmpbase);
  if (!mkdtemp(c->dir)) { c->dir[0] = 0; respond_err(c, 500, "internal server error"); return; }

  /* req.txt: request line + headers, CRLF -> LF, no blank line at the end (every line LF-terminated) */
  size_t hl = c->hdrlen - 4, k = 0; char *req = malloc(hl + 2);
  for (size_t i = 0; i < hl; i++) if (!(c->in[i] == '\r' && i + 1 < hl + 2 && c->in[i + 1] == '\n')) req[k++] = c->in[i];
  req[k++] = '\n';
  char meta[PATH_MAX + 512]; const char *secret = getenv("JWT_SECRET");
  int ml = snprintf(meta, sizeof meta, "%ld\n%ld\n%s\n", (long)time(NULL), (long)start_time, secret ? secret : "");
  int bad = write_file(c->dir, "meta.txt", meta, ml) || write_file(c->dir, "req.txt", req, k)
         || write_file(c->dir, "body.txt", c->in + c->hdrlen, c->clen);
  free(req);
  if (!bad) {
    char p1[PATH_MAX + 16], p2[PATH_MAX + 16];
    snprintf(p1, sizeof p1, "%s/db.req", c->dir); snprintf(p2, sizeof p2, "%s/db.resp", c->dir);
    if (mkfifo(p1, 0600) || mkfifo(p2, 0600)) bad = 1;
    else {
      /* db.req read-write so the host is also a writer (no POLLHUP games); db.resp: a keeper reader first, so the
         non-blocking writer open works and unread data survives until TeX opens it */
      c->reqfd = open(p1, O_RDWR | O_NONBLOCK);
      c->keepfd = open(p2, O_RDONLY | O_NONBLOCK);
      c->respfd = c->keepfd >= 0 ? open(p2, O_WRONLY | O_NONBLOCK) : -1;
      if (c->reqfd < 0 || c->keepfd < 0 || c->respfd < 0) bad = 1;
      else { set_nb_cloexec(c->reqfd); set_nb_cloexec(c->keepfd); set_nb_cloexec(c->respfd); }
    }
  }
  if (bad) { db_release(c); rm_rf(c->dir); c->dir[0] = 0; respond_err(c, 500, "internal server error"); return; }
  c->dbst = DB_WAITLINE; c->dblen = 0;

  char fmtarg[PATH_MAX + 32]; snprintf(fmtarg, sizeof fmtarg, "-fmt=%s", fmtpath);
  pid_t pid = fork();
  if (pid < 0) { db_release(c); rm_rf(c->dir); respond_err(c, 500, "internal server error"); return; }
  if (pid == 0) {
    signal(SIGPIPE, SIG_DFL);
    int dn = open("/dev/null", O_RDWR);
    if (dn >= 0) { dup2(dn, 0); dup2(dn, 1); dup2(dn, 2); }
    if (chdir(c->dir) != 0) _exit(127);
    setenv("max_print_line", "1000000", 1); setenv("error_line", "254", 1); setenv("half_error_line", "238", 1);
    setenv("openout_any", "a", 1); setenv("openin_any", "a", 1);
    execlp("pdftex", "pdftex", fmtarg, "-8bit", "-interaction=batchmode", "-halt-on-error",
           "\\feedmain", (char *)NULL);
    _exit(127);
  }
  c->pid = pid; c->started = time(NULL); c->st = RUNNING; running++;
}

/* Reads <dir>/resp.txt and answers. */
static void finish(conn *c, int status) {
  char path[PATH_MAX + 16], buf[1 << 20]; size_t n = 0;
  snprintf(path, sizeof path, "%s/resp.txt", c->dir);
  int ok = WIFEXITED(status) && WEXITSTATUS(status) == 0;
  FILE *f = ok ? fopen(path, "rb") : NULL;
  if (f) { n = fread(buf, 1, sizeof buf, f); fclose(f); }
  db_release(c);
  rm_rf(c->dir); c->pid = 0; c->dir[0] = 0; running--;
  int code = 0;
  if (n >= 5 && buf[3] == '\n' && buf[0] >= '1' && buf[0] <= '5' && buf[1] >= '0' && buf[1] <= '9' && buf[2] >= '0' && buf[2] <= '9')
    code = (buf[0] - '0') * 100 + (buf[1] - '0') * 10 + (buf[2] - '0');
  if (!code) { respond(c, 500, BODY500, strlen(BODY500)); return; }
  size_t bl = n - 4;
  while (bl && (buf[4 + bl - 1] == '\n' || buf[4 + bl - 1] == '\r')) bl--;
  if (!bl) { respond(c, 500, BODY500, strlen(BODY500)); return; }
  respond(c, code, buf + 4, bl);
}

static void reap(void) {
  char tmp[64];
  while (read(selfpipe[0], tmp, sizeof tmp) > 0) {}
  int status; pid_t pid;
  while ((pid = waitpid(-1, &status, WNOHANG)) > 0)
    for (size_t i = 0; i < nconns; i++)
      if (conns[i]->st == RUNNING && conns[i]->pid == pid) { finish(conns[i], status); break; }
}

static void dispatch(void) {
  while (running < max_children) {
    conn *best = NULL;
    for (size_t i = 0; i < nconns; i++)
      if (conns[i]->st == QUEUED && (!best || conns[i]->seq < best->seq)) best = conns[i];
    if (!best) return;
    spawn(best);
  }
}

/* ---------- main ---------- */

static int listen_on(const char *host, const char *port) {
  struct addrinfo hints = {0}, *ai;
  hints.ai_family = AF_UNSPEC; hints.ai_socktype = SOCK_STREAM; hints.ai_flags = AI_PASSIVE;
  if (getaddrinfo(host, port, &hints, &ai) != 0) { fprintf(stderr, "bad HOST/PORT %s:%s\n", host, port); exit(1); }
  int fd = socket(ai->ai_family, ai->ai_socktype, 0);
  if (fd < 0) die("socket");
  int one = 1; setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
  if (bind(fd, ai->ai_addr, ai->ai_addrlen) < 0) die("bind");
  if (listen(fd, 4096) < 0) die("listen");
  set_nb_cloexec(fd); freeaddrinfo(ai);
  return fd;
}

int main(int argc, char **argv) {
  (void)argc;
  const char *host = getenv("HOST"), *port = getenv("PORT"), *extra = getenv("EXTRA_PORT");
  if (!host || !*host) host = "0.0.0.0";
  if (!port || !*port) port = "3000";
  find_paths(argv[0]);
  start_time = time(NULL);
  long nc = sysconf(_SC_NPROCESSORS_ONLN);
  max_children = 4 * (nc > 0 ? (int)nc : 1);
  if (getenv("MAX_CHILDREN") && atoi(getenv("MAX_CHILDREN")) > 0) max_children = atoi(getenv("MAX_CHILDREN"));
  if (max_children > 512) max_children = 512;

  signal(SIGPIPE, SIG_IGN);
  if (pipe(selfpipe) < 0) die("pipe");
  set_nb_cloexec(selfpipe[0]); set_nb_cloexec(selfpipe[1]);
  struct sigaction sa = {0}; sa.sa_handler = on_chld; sa.sa_flags = SA_RESTART | SA_NOCLDSTOP;
  sigemptyset(&sa.sa_mask); sigaction(SIGCHLD, &sa, NULL);

  int lfd[2], nl = 0;
  lfd[nl++] = listen_on(host, port);
  if (extra && *extra && strcmp(extra, port)) lfd[nl++] = listen_on(host, extra);
  fprintf(stderr, "host: listening on %s:%s%s%s, max %d children, fmt %s\n", host, port, nl > 1 ? " and " : "",
          nl > 1 ? extra : "", max_children, fmtpath);

  db_init();
  struct pollfd *pfds = NULL; conn **owner = NULL; int *kind = NULL; dbc **downer = NULL; size_t cap = 0;
  enum { K_SELF, K_LISTEN, K_CLIENT, K_REQ, K_RESP, K_DB };
  for (;;) {
    size_t need = nconns * 2 + npool + 8;
    if (cap < need) {
      cap = need * 2;
      pfds = realloc(pfds, cap * sizeof *pfds); owner = realloc(owner, cap * sizeof *owner);
      kind = realloc(kind, cap * sizeof *kind); downer = realloc(downer, cap * sizeof *downer);
    }
    size_t np = 0;
#define ADD(FD, EV, K, C, D) do { pfds[np].fd = (FD); pfds[np].events = (EV); pfds[np].revents = 0; kind[np] = (K); owner[np] = (C); downer[np] = (D); np++; } while (0)
    ADD(selfpipe[0], POLLIN, K_SELF, NULL, NULL);
    for (int i = 0; i < nl; i++) ADD(lfd[i], POLLIN, K_LISTEN, NULL, NULL);
    int any_down = 0;
    for (int i = 0; i < npool; i++) {
      dbc *d = &pool[i];
      if (d->st == D_DOWN) { any_down = 1; continue; }
      short ev = d->st == D_CONNECTING ? d->polldir : (POLLIN | (d->wantout ? POLLOUT : 0));
      int sfd = PQsocket(d->pg);
      if (sfd < 0) { dbc_fail(d, "connection lost"); any_down = 1; continue; }
      ADD(sfd, ev, K_DB, NULL, d);
    }
    for (size_t i = 0; i < nconns; i++) {
      conn *c = conns[i];
      if (c->st == READING || c->st == WRITING) ADD(c->fd, c->st == READING ? POLLIN : POLLOUT, K_CLIENT, c, NULL);
      else if (c->st == RUNNING && c->dbst == DB_WAITLINE) ADD(c->reqfd, POLLIN, K_REQ, c, NULL);
      else if (c->st == RUNNING && c->dbst == DB_RESP) ADD(c->respfd, POLLOUT, K_RESP, c, NULL);
    }
    int r = poll(pfds, np, any_down ? 50 : 1000);
    if (r < 0 && errno != EINTR) die("poll");
    if (r > 0) {
      for (size_t i = 0; i < np; i++) {
        short rev = pfds[i].revents;
        if (!rev) continue;
        conn *c = owner[i];
        switch (kind[i]) {
          case K_SELF: reap(); break;
          case K_LISTEN: accept_on(pfds[i].fd); break;
          case K_DB: if (downer[i]->st != D_DOWN && PQsocket(downer[i]->pg) == pfds[i].fd) dbc_event(downer[i], rev); break;
          case K_REQ: if (c->st == RUNNING && c->dbst == DB_WAITLINE) db_read_line(c); break;
          case K_RESP: if (c->st == RUNNING && c->dbst == DB_RESP) db_try_write(c); break;
          default:
            if (c->dead) break;
            if (c->st == READING && (rev & (POLLIN | POLLHUP | POLLERR))) on_readable(c);
            else if (c->st == WRITING && (rev & (POLLOUT | POLLERR | POLLHUP))) flush_out(c);
            else if (rev & POLLNVAL) close_conn(c);
        }
      }
    }
    db_dispatch();
    dispatch();
    time_t now = time(NULL);
    for (size_t i = 0; i < nconns; i++) {
      conn *c = conns[i];
      if (c->st == READING && now - c->last > IDLE_SECS) close_conn(c);
      else if (c->st == WRITING && now - c->last > IDLE_SECS) close_conn(c);
      else if (c->st == RUNNING && now - c->started > CHILD_SECS) kill(c->pid, SIGKILL);
    }
    compact();
  }
}
