#!/bin/sh
set -eu

/scripts/wait-for-mongo.sh localhost 27019

mongosh --quiet --host localhost --port 27019 <<'JS'
try {
  const status = rs.status();
  if (status.ok === 1) {
    print('Config server replica set already initialized');
    quit(0);
  }
} catch (e) {
  // not initialized yet
}

rs.initiate({
  _id: 'cfgReplSet',
  configsvr: true,
  members: [
    { _id: 0, host: 'cfg1:27019' },
    { _id: 1, host: 'cfg2:27019' },
    { _id: 2, host: 'cfg3:27019' }
  ]
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
if (!ready) throw new Error('Config server replica set did not become primary in time');
JS
