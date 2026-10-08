#!/bin/sh
set -eu

: "${MONGO_ROOT_USERNAME:?MONGO_ROOT_USERNAME is required}"
: "${MONGO_ROOT_PASSWORD:?MONGO_ROOT_PASSWORD is required}"
: "${MONGO_APP_DB:?MONGO_APP_DB is required}"

/scripts/wait-for-mongo.sh mongos 27017 "$MONGO_ROOT_USERNAME" "$MONGO_ROOT_PASSWORD" admin

MONGO_APP_DB="$MONGO_APP_DB" \
  mongosh --quiet --host mongos --port 27017 -u "$MONGO_ROOT_USERNAME" -p "$MONGO_ROOT_PASSWORD" --authenticationDatabase admin <<'JS'
const dbx = db.getSiblingDB(process.env.MONGO_APP_DB);

print('Normalizing businesses.categories');
dbx.businesses.updateMany(
  { categories: { $type: 'string' } },
  [{
    $set: {
      categories: {
        $filter: {
          input: { $split: ['$categories', ', '] },
          as: 'cat',
          cond: { $ne: ['$$cat', ''] }
        }
      }
    }
  }]
);

print('Normalizing users.yelping_since, users.friends, users.elite');
dbx.users.updateMany(
  {},
  [{
    $set: {
      yelping_since: {
        $cond: [
          { $eq: [{ $type: '$yelping_since' }, 'string'] },
          { $dateFromString: { dateString: '$yelping_since', onError: '$yelping_since', onNull: '$yelping_since' } },
          '$yelping_since'
        ]
      },
      friends: {
        $switch: {
          branches: [
            { case: { $eq: [{ $type: '$friends' }, 'array'] }, then: '$friends' },
            { case: { $or: [ { $eq: ['$friends', null] }, { $eq: ['$friends', 'None'] }, { $eq: ['$friends', ''] } ] }, then: [] }
          ],
          default: { $split: ['$friends', ', '] }
        }
      },
      elite: {
        $switch: {
          branches: [
            { case: { $eq: [{ $type: '$elite' }, 'array'] }, then: '$elite' },
            { case: { $or: [ { $eq: ['$elite', null] }, { $eq: ['$elite', 'None'] }, { $eq: ['$elite', ''] } ] }, then: [] }
          ],
          default: { $split: ['$elite', ','] }
        }
      }
    }
  }]
);

print('Normalizing reviews.date');
dbx.reviews.updateMany(
  { date: { $type: 'string' } },
  [{
    $set: {
      date: { $dateFromString: { dateString: '$date', onError: '$date', onNull: '$date' } }
    }
  }]
);
JS
