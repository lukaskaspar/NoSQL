#!/bin/sh
set -eu

mkdir -p /shared

if [ ! -f /shared/mongodb-keyfile ]; then
  python3 - <<'PY2'
import base64, os
secret = base64.b64encode(os.urandom(768)).decode('ascii')
with open('/shared/mongodb-keyfile', 'w', encoding='utf-8') as fh:
    fh.write(secret)
PY2
fi

chmod 400 /shared/mongodb-keyfile
