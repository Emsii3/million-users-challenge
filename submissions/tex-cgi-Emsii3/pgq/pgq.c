/* pgq: one SPEC statement per invocation, line-oriented output for the TeX handler (see PLAN Contract 2).
 *   pgq health | feed | post <id> | create <uid> <bodyfile> | like <uid> <postid>
 * Strings (username, body, error messages) are printed as lowercase hex of their raw bytes. Exit 0 always. */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libpq-fe.h>

#define POST_SELECT \
  "SELECT p.id, p.body, p.created_at, u.username,\n" \
  "       (SELECT count(*) FROM likes l WHERE l.post_id = p.id)::int AS like_count\n" \
  "  FROM posts p JOIN users u ON u.id = p.user_id"

static void hex(const char *s) {
  for (; *s; s++) printf("%02x", (unsigned char)*s);
}

static void err(const char *state, const char *msg) {
  printf("ERR|%s|", state && *state ? state : "CONN");
  if (msg) {
    /* libpq messages end with a newline: drop it */
    size_t n = strlen(msg);
    while (n && (msg[n - 1] == '\n' || msg[n - 1] == '\r')) n--;
    for (size_t i = 0; i < n; i++) printf("%02x", (unsigned char)msg[i]);
  }
  printf("\n");
}

static void result_err(PGresult *r, PGconn *c) {
  const char *st = PQresultErrorField(r, PG_DIAG_SQLSTATE);
  err(st ? st : "CONN", PQresultErrorMessage(r) ? PQresultErrorMessage(r) : PQerrorMessage(c));
}

static void print_posts(PGresult *r) {
  for (int i = 0; i < PQntuples(r); i++) {
    printf("P|%s|%s|", PQgetvalue(r, i, 0), PQgetvalue(r, i, 2));
    hex(PQgetvalue(r, i, 3));
    printf("|%s|", PQgetvalue(r, i, 4));
    hex(PQgetvalue(r, i, 1));
    printf("\n");
  }
}

static char *read_file(const char *path) {
  FILE *f = fopen(path, "rb");
  if (!f) return NULL;
  size_t cap = 4096, n = 0;
  char *b = malloc(cap + 1);
  size_t k;
  while ((k = fread(b + n, 1, cap - n, f)) > 0) {
    n += k;
    if (n == cap) b = realloc(b, (cap *= 2) + 1);
  }
  fclose(f);
  if (n && b[n - 1] == '\n') n--; /* strip ONE trailing newline */
  b[n] = 0;
  return b;
}

int main(int argc, char **argv) {
  if (argc < 2) { err("USAGE", "usage: pgq cmd args"); return 0; }
  const char *cmd = argv[1];
  const char *url = getenv("DATABASE_URL");
  const char *keys[] = {"dbname", "options", NULL};
  const char *vals[] = {url && *url ? url : NULL, "-c TimeZone=UTC -c DateStyle=ISO", NULL};
  PGconn *c = PQconnectdbParams(keys, vals, 1);
  if (PQstatus(c) != CONNECTION_OK) { err("CONN", PQerrorMessage(c)); PQfinish(c); return 0; }

  PGresult *r = NULL;
  if (!strcmp(cmd, "health")) {
    r = PQexec(c, "SELECT 1");
    if (PQresultStatus(r) == PGRES_TUPLES_OK) puts("OK"); else result_err(r, c);
  } else if (!strcmp(cmd, "feed")) {
    r = PQexecParams(c, POST_SELECT "\n ORDER BY p.created_at DESC, p.id DESC LIMIT 20", 0, NULL, NULL, NULL, NULL, 0);
    if (PQresultStatus(r) == PGRES_TUPLES_OK) print_posts(r); else result_err(r, c);
  } else if (!strcmp(cmd, "post") && argc >= 3) {
    const char *p[] = {argv[2]};
    r = PQexecParams(c, POST_SELECT "\n WHERE p.id = $1", 1, NULL, p, NULL, NULL, 0);
    if (PQresultStatus(r) == PGRES_TUPLES_OK) print_posts(r); else result_err(r, c);
  } else if (!strcmp(cmd, "create") && argc >= 4) {
    char *body = read_file(argv[3]);
    if (!body) { err("FILE", "cannot read body file"); goto done; }
    const char *p[] = {argv[2], body};
    r = PQexecParams(c, "INSERT INTO posts (user_id, body) VALUES ($1, $2) RETURNING id, created_at",
                     2, NULL, p, NULL, NULL, 0);
    if (PQresultStatus(r) == PGRES_TUPLES_OK && PQntuples(r) == 1)
      printf("C|%s|%s\n", PQgetvalue(r, 0, 0), PQgetvalue(r, 0, 1));
    else result_err(r, c);
    free(body);
  } else if (!strcmp(cmd, "like") && argc >= 4) {
    const char *p[] = {argv[2], argv[3]};
    r = PQexecParams(c, "INSERT INTO likes (user_id, post_id) VALUES ($1, $2) ON CONFLICT (user_id, post_id) DO NOTHING",
                     2, NULL, p, NULL, NULL, 0);
    if (PQresultStatus(r) == PGRES_COMMAND_OK) printf("L|%s\n", strcmp(PQcmdTuples(r), "0") ? "1" : "0");
    else result_err(r, c);
  } else {
    err("USAGE", "unknown command");
  }
done:
  if (r) PQclear(r);
  PQfinish(c);
  return 0;
}
