#!/bin/sh
set -eu

: "${MONGO_ROOT_USERNAME:?MONGO_ROOT_USERNAME is required}"
: "${MONGO_ROOT_PASSWORD:?MONGO_ROOT_PASSWORD is required}"

/scripts/wait-for-mongo.sh localhost 27017

# First create the initial admin user using localhost exception on mongos.
MONGO_ROOT_USERNAME="$MONGO_ROOT_USERNAME" MONGO_ROOT_PASSWORD="$MONGO_ROOT_PASSWORD" \
  mongosh --quiet --host localhost --port 27017 <<'JS'
const admin = db.getSiblingDB('admin');
const rootUser = process.env.MONGO_ROOT_USERNAME;
const rootPass = process.env.MONGO_ROOT_PASSWORD;

try {
  admin.createUser({
    user: rootUser,
    pwd: rootPass,
    roles: [{ role: 'root', db: 'admin' }]
  });
  print('Root user created');
} catch (e) {
  const msg = String(e.message || '');
  const codeName = String(e.codeName || '');
  if (msg.includes('already exists') || codeName.includes('Duplicate')) {
    print('Root user already exists');
  } else {
    throw e;
  }
}
JS

sleep 5
/scripts/wait-for-mongo.sh localhost 27017 "$MONGO_ROOT_USERNAME" "$MONGO_ROOT_PASSWORD" admin 60

# Then authenticate and add shards.
MONGO_ROOT_USERNAME="$MONGO_ROOT_USERNAME" MONGO_ROOT_PASSWORD="$MONGO_ROOT_PASSWORD" \
  mongosh --quiet --host localhost --port 27017 -u "$MONGO_ROOT_USERNAME" -p "$MONGO_ROOT_PASSWORD" --authenticationDatabase admin <<'JS'
function ensureShard(name, connString) {
  const result = db.adminCommand({ listShards: 1 });
  const existing = result.shards || [];
  if (!existing.find(s => s._id === name)) {
    print('Adding shard ' + name);
    sh.addShard(connString);
  } else {
    print('Shard ' + name + ' already exists');
  }
}

ensureShard('shard01', 'shard01/shard1a:27018,shard1b:27018,shard1c:27018');
ensureShard('shard02', 'shard02/shard2a:27018,shard2b:27018,shard2c:27018');
ensureShard('shard03', 'shard03/shard3a:27018,shard3b:27018,shard3c:27018');
JS
