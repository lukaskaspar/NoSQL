#!/bin/sh
set -eu

docker exec -i mongos mongosh -u "${MONGO_ROOT_USERNAME:-clusterAdmin}" -p "${MONGO_ROOT_PASSWORD:-ChangeMeRoot123!}" --authenticationDatabase admin --quiet <<'JS'
const dbx = db.getSiblingDB('yelp');
printjson({
  businesses: dbx.businesses.countDocuments(),
  users: dbx.users.countDocuments(),
  reviews: dbx.reviews.countDocuments()
});
JS
