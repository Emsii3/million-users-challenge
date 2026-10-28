/* host: a tiny HTTP/1.1 front for the TeX CGI handler (see PLAN Contract 1).
 * One poll() loop, two listening ports, keep-alive connections. Each complete request is written to a fresh temp
 * dir and handled by one `pdftex -fmt=feed '\feedmain'` process (at most MAX_CHILDREN at once, the rest queue).
 * The host knows no application logic: it frames HTTP, runs TeX, and relays resp.txt. */
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
#ifdef __APPLE__
#include <mach-o/dyld.h>
#endif

#define MAX_BODY 16384
#define MAX_HEAD 16384
#define IDLE_SECS 120
#define CHILD_SECS 30
#define BODY500 "{\"error\":\"internal server error\"}"

enum state { READING, QUEUED, RUNNING, WRITING };

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
} conn;

static conn **conns; static size_t nconns, capconns;
static int running, max_children, selfpipe[2];
static unsigned long seqno;
static time_t start_time;
static char tmpbase[PATH_MAX], fmtpath[PATH_MAX + 16], pgqpath[PATH_MAX];

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
  snprintf(pgqpath, sizeof pgqpath, "%s/pgq", exe);
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
  int ml = snprintf(meta, sizeof meta, "%ld\n%ld\n%s\n%s\n", (long)time(NULL), (long)start_time, secret ? secret : "", pgqpath);
  int bad = write_file(c->dir, "meta.txt", meta, ml) || write_file(c->dir, "req.txt", req, k)
         || write_file(c->dir, "body.txt", c->in + c->hdrlen, c->clen);
  free(req);
  if (bad) { rm_rf(c->dir); respond_err(c, 500, "internal server error"); return; }

  char fmtarg[PATH_MAX + 32]; snprintf(fmtarg, sizeof fmtarg, "-fmt=%s", fmtpath);
  pid_t pid = fork();
  if (pid < 0) { rm_rf(c->dir); respond_err(c, 500, "internal server error"); return; }
  if (pid == 0) {
    signal(SIGPIPE, SIG_DFL);
    int dn = open("/dev/null", O_RDWR);
    if (dn >= 0) { dup2(dn, 0); dup2(dn, 1); dup2(dn, 2); }
    if (chdir(c->dir) != 0) _exit(127);
    setenv("max_print_line", "1000000", 1); setenv("error_line", "254", 1); setenv("half_error_line", "238", 1);
    setenv("openout_any", "a", 1); setenv("openin_any", "a", 1);
    execlp("pdftex", "pdftex", fmtarg, "-8bit", "-shell-escape", "-interaction=batchmode", "-halt-on-error",
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
  if (max_children > 512) max_children = 512; /* one Postgres connection per child */

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

  struct pollfd *pfds = NULL; conn **owner = NULL; size_t cap = 0;
  for (;;) {
    if (cap < nconns + 3) {
      cap = (nconns + 3) * 2;
      pfds = realloc(pfds, cap * sizeof *pfds); owner = realloc(owner, cap * sizeof *owner);
    }
    size_t np = 0;
    pfds[np].fd = selfpipe[0]; pfds[np].events = POLLIN; owner[np++] = NULL;
    for (int i = 0; i < nl; i++) { pfds[np].fd = lfd[i]; pfds[np].events = POLLIN; owner[np++] = NULL; }
    size_t first_conn = np;
    for (size_t i = 0; i < nconns; i++) {
      conn *c = conns[i];
      if (c->st == READING || c->st == WRITING) {
        pfds[np].fd = c->fd; pfds[np].events = c->st == READING ? POLLIN : POLLOUT; owner[np++] = c;
      }
    }
    int r = poll(pfds, np, 1000);
    if (r < 0 && errno != EINTR) die("poll");
    if (r > 0) {
      if (pfds[0].revents) reap();
      for (int i = 0; i < nl; i++) if (pfds[1 + i].revents) accept_on(lfd[i]);
      for (size_t i = first_conn; i < np; i++) {
        conn *c = owner[i];
        if (!pfds[i].revents || c->dead) continue;
        if (c->st == READING && (pfds[i].revents & (POLLIN | POLLHUP | POLLERR))) on_readable(c);
        else if (c->st == WRITING && (pfds[i].revents & (POLLOUT | POLLERR | POLLHUP))) flush_out(c);
        else if (pfds[i].revents & (POLLNVAL)) close_conn(c);
      }
    }
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
