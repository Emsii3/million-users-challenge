#!/usr/bin/env bash
# Foreground server. Env: DATABASE_URL, JWT_SECRET, HOST, PORT, EXTRA_PORT, [MAX_CHILDREN] (default 4 x cores).
set -euo pipefail
cd "$(dirname "$0")"
exec bin/host
