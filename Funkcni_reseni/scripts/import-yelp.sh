#!/bin/sh
set -eu
: "${MONGO_ROOT_USERNAME:?MONGO_ROOT_USERNAME is required}"
: "${MONGO_ROOT_PASSWORD:?MONGO_ROOT_PASSWORD is required}"
: "${MONGO_APP_DB:?MONGO_APP_DB is required}"
: "${YELP_BUSINESS_FILE:?YELP_BUSINESS_FILE is required}"
: "${YELP_USER_FILE:?YELP_USER_FILE is required}"
: "${YELP_REVIEW_FILE:?YELP_REVIEW_FILE is required}"

for f in "$YELP_BUSINESS_FILE" "$YELP_USER_FILE" "$YELP_REVIEW_FILE"; do
  if [ ! -f "$f" ]; then
    echo "Missing data file: $f" >&2
    exit 1
  fi
done

if ! command -v mongoimport >/dev/null 2>&1; then
  echo "mongoimport is not installed in the helper image." >&2
  exit 1
fi

json_flag() {
  file="$1"
  first_char="$(dd if="$file" bs=1 count=4096 2>/dev/null | tr -d '[:space:]' | head -c 1)"
  if [ "$first_char" = "[" ]; then
    printf '%s' '--jsonArray'
  fi
}

/scripts/wait-for-mongo.sh mongos 27017 "$MONGO_ROOT_USERNAME" "$MONGO_ROOT_PASSWORD" admin

MONGO_APP_DB="$MONGO_APP_DB" \
  mongosh --quiet --host mongos --port 27017 -u "$MONGO_ROOT_USERNAME" -p "$MONGO_ROOT_PASSWORD" --authenticationDatabase admin <<'JS'
const dbx = db.getSiblingDB(process.env.MONGO_APP_DB);
dbx.businesses.deleteMany({});
dbx.users.deleteMany({});
dbx.reviews.deleteMany({});
JS

BFLAG="$(json_flag "$YELP_BUSINESS_FILE")"
UFLAG="$(json_flag "$YELP_USER_FILE")"
RFLAG="$(json_flag "$YELP_REVIEW_FILE")"

mongoimport --host mongos --port 27017 \
  -u "$MONGO_ROOT_USERNAME" -p "$MONGO_ROOT_PASSWORD" --authenticationDatabase admin \
  --db "$MONGO_APP_DB" --collection businesses \
  --file "$YELP_BUSINESS_FILE" \
  ${BFLAG:+$BFLAG }--numInsertionWorkers 4

mongoimport --host mongos --port 27017 \
  -u "$MONGO_ROOT_USERNAME" -p "$MONGO_ROOT_PASSWORD" --authenticationDatabase admin \
  --db "$MONGO_APP_DB" --collection users \
  --file "$YELP_USER_FILE" \
  ${UFLAG:+$UFLAG }--numInsertionWorkers 4

mongoimport --host mongos --port 27017 \
  -u "$MONGO_ROOT_USERNAME" -p "$MONGO_ROOT_PASSWORD" --authenticationDatabase admin \
  --db "$MONGO_APP_DB" --collection reviews \
  --file "$YELP_REVIEW_FILE" \
  ${RFLAG:+$RFLAG }--numInsertionWorkers 8
