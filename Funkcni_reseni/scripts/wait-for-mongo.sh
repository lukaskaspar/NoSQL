#!/bin/sh
set -eu

HOST="${1:?host required}"
PORT="${2:?port required}"
USER="${3:-}"
PASS="${4:-}"
AUTH_DB="${5:-admin}"
TRIES="${6:-180}"

count=0
while [ "$count" -lt "$TRIES" ]; do
  if [ -n "$USER" ] && [ -n "$PASS" ]; then
    if mongosh --quiet --host "$HOST" --port "$PORT" -u "$USER" -p "$PASS" --authenticationDatabase "$AUTH_DB" --eval 'db.adminCommand({ ping: 1 }).ok' >/dev/null 2>&1; then
      exit 0
    fi
  else
    if mongosh --quiet --host "$HOST" --port "$PORT" --eval 'db.adminCommand({ ping: 1 }).ok' >/dev/null 2>&1; then
      exit 0
    fi
  fi
  count=$((count + 1))
  sleep 2
done

echo "MongoDB $HOST:$PORT not ready after $TRIES attempts" >&2
exit 1
