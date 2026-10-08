#!/bin/sh
set -eu
: "${MONGO_ROOT_USERNAME:?MONGO_ROOT_USERNAME is required}"
: "${MONGO_ROOT_PASSWORD:?MONGO_ROOT_PASSWORD is required}"
: "${MONGO_APP_DB:?MONGO_APP_DB is required}"
: "${MONGO_APP_USER:?MONGO_APP_USER is required}"
: "${MONGO_APP_PASSWORD:?MONGO_APP_PASSWORD is required}"
/scripts/wait-for-mongo.sh localhost 27017 "$MONGO_ROOT_USERNAME" "$MONGO_ROOT_PASSWORD" admin

MONGO_APP_DB="$MONGO_APP_DB" MONGO_APP_USER="$MONGO_APP_USER" MONGO_APP_PASSWORD="$MONGO_APP_PASSWORD" \
  mongosh --quiet --host localhost --port 27017 -u "$MONGO_ROOT_USERNAME" -p "$MONGO_ROOT_PASSWORD" --authenticationDatabase admin <<'JS'
const appDbName = process.env.MONGO_APP_DB;
const appUser = process.env.MONGO_APP_USER;
const appPass = process.env.MONGO_APP_PASSWORD;
const appDb = db.getSiblingDB(appDbName);

function ensureCollectionWithValidator(name, validator) {
  const exists = appDb.getCollectionInfos({ name }).length > 0;
  if (!exists) {
    print('Creating collection ' + name);
    appDb.createCollection(name, {
      validator,
      validationLevel: 'strict',
      validationAction: 'error'
    });
  } else {
    print('Updating validator for ' + name);
    appDb.runCommand({
      collMod: name,
      validator,
      validationLevel: 'strict',
      validationAction: 'error'
    });
  }
}

ensureCollectionWithValidator('businesses', {
  $jsonSchema: {
    bsonType: 'object',
    required: ['business_id', 'name', 'city', 'state', 'stars', 'review_count', 'is_open'],
    properties: {
      business_id: { bsonType: 'string' },
      name: { bsonType: 'string' },
      city: { bsonType: 'string' },
      state: { bsonType: 'string' },
      stars: { bsonType: ['double', 'int', 'long', 'decimal'] },
      review_count: { bsonType: ['int', 'long'] },
      is_open: { bsonType: ['int', 'bool'] },
      categories: { bsonType: ['array', 'string', 'null'] },
      attributes: { bsonType: ['object', 'null'] },
      hours: { bsonType: ['object', 'null'] }
    }
  }
});

ensureCollectionWithValidator('users', {
  $jsonSchema: {
    bsonType: 'object',
    required: ['user_id', 'name', 'review_count', 'average_stars'],
    properties: {
      user_id: { bsonType: 'string' },
      name: { bsonType: 'string' },
      review_count: { bsonType: ['int', 'long'] },
      average_stars: { bsonType: ['double', 'int', 'long', 'decimal'] },
      yelping_since: { bsonType: ['date', 'string', 'null'] },
      friends: { bsonType: ['array', 'string', 'null'] },
      elite: { bsonType: ['array', 'string', 'null'] },
      fans: { bsonType: ['int', 'long', 'null'] }
    }
  }
});

ensureCollectionWithValidator('reviews', {
  $jsonSchema: {
    bsonType: 'object',
    required: ['review_id', 'user_id', 'business_id', 'stars', 'text', 'date'],
    properties: {
      review_id: { bsonType: 'string' },
      user_id: { bsonType: 'string' },
      business_id: { bsonType: 'string' },
      stars: { bsonType: ['double', 'int', 'long', 'decimal'] },
      text: { bsonType: 'string' },
      useful: { bsonType: ['int', 'long', 'null'] },
      funny: { bsonType: ['int', 'long', 'null'] },
      cool: { bsonType: ['int', 'long', 'null'] },
      date: { bsonType: ['date', 'string'] }
    }
  }
});

try {
  sh.enableSharding(appDbName);
} catch (e) {
  print('enableSharding skipped: ' + e.message);
}

function ensureSharded(ns, key) {
  const coll = db.getSiblingDB('config').collections.findOne({ _id: ns });
  if (!coll || !coll.key) {
    print('Sharding collection ' + ns);
    sh.shardCollection(ns, key);
  } else {
    print(ns + ' already sharded');
  }
}

ensureSharded(appDbName + '.businesses', { business_id: 'hashed' });
ensureSharded(appDbName + '.users', { user_id: 'hashed' });
ensureSharded(appDbName + '.reviews', { business_id: 'hashed' });

if (!appDb.getUser(appUser)) {
  appDb.createUser({
    user: appUser,
    pwd: appPass,
    roles: [
      { role: 'readWrite', db: appDbName },
      { role: 'dbAdmin', db: appDbName },
      { role: 'read', db: 'config' }
    ]
  });
} else {
  print('App user already exists');
}
JS
