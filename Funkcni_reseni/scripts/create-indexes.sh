#!/bin/sh
set -eu
: "${MONGO_ROOT_USERNAME:?MONGO_ROOT_USERNAME is required}"
: "${MONGO_ROOT_PASSWORD:?MONGO_ROOT_PASSWORD is required}"
: "${MONGO_APP_DB:?MONGO_APP_DB is required}"
/scripts/wait-for-mongo.sh localhost 27017 "$MONGO_ROOT_USERNAME" "$MONGO_ROOT_PASSWORD" admin

MONGO_APP_DB="$MONGO_APP_DB" \
  mongosh --quiet --host localhost --port 27017 -u "$MONGO_ROOT_USERNAME" -p "$MONGO_ROOT_PASSWORD" --authenticationDatabase admin <<'JS'
const appDb = db.getSiblingDB(process.env.MONGO_APP_DB);
appDb.businesses.createIndex({ city: 1, state: 1, stars: -1 }, { name: 'idx_city_state_stars' });
appDb.businesses.createIndex({ categories: 1 }, { name: 'idx_categories' });
appDb.businesses.createIndex({ review_count: -1 }, { name: 'idx_business_review_count' });
appDb.users.createIndex({ review_count: -1, average_stars: -1 }, { name: 'idx_user_activity' });
appDb.users.createIndex({ yelping_since: 1 }, { name: 'idx_yelping_since' });
appDb.users.createIndex({ fans: -1 }, { name: 'idx_user_fans' });
appDb.reviews.createIndex({ business_id: 1, date: -1 }, { name: 'idx_business_date' });
appDb.reviews.createIndex({ user_id: 1, date: -1 }, { name: 'idx_user_date' });
appDb.reviews.createIndex({ stars: -1, useful: -1 }, { name: 'idx_review_rank' });
appDb.reviews.createIndex({ business_id: 1, text: 'text' }, { name: 'txt_business_text' });
JS
