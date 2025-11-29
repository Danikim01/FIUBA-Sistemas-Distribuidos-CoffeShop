#!/usr/bin/env bash
set -euo pipefail

docker ps --format '{{.ID}} {{.Names}}' | while read -r cid name; do
  if docker exec "$cid" bash -lc '
    STATE_DIR="/app/src/workers/state"
    [ -d "$STATE_DIR" ] || exit 1
    find "$STATE_DIR" -type f -print -quit | grep -q . || exit 1
  '; then
    echo "$name"
  fi
done
