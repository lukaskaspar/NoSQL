#!/bin/sh
set -eu

: "${RS_NAME:?RS_NAME is required}"
: "${RS_PORT:?RS_PORT is required}"
: "${RS_MEMBERS:?RS_MEMBERS is required}"

/scripts/wait-for-mongo.sh localhost "$RS_PORT"

RS_NAME="$RS_NAME" RS_MEMBERS="$RS_MEMBERS" mongosh --quiet --host localhost --port "$RS_PORT" <<'JS'
const rsName = process.env.RS_NAME;
const members = process.env.RS_MEMBERS.split(',').map((host, idx) => ({ _id: idx, host }));

try {
  const status = rs.status();
  if (status.ok === 1) {
    print('Replica set ' + rsName + ' already initialized');
    quit(0);
  }
} catch (e) {
  // not initialized yet
}

rs.initiate({
  _id: rsName,
  members
});

let ready = false;
for (let i = 0; i < 60; i++) {
  try {
    const hello = db.adminCommand({ hello: 1 });
    if (hello.isWritablePrimary) {
      ready = true;
      break;
    }
  } catch (e) {}
  sleep(2000);
}
if (!ready) throw new Error('Replica set ' + rsName + ' did not become primary in time');
JS
