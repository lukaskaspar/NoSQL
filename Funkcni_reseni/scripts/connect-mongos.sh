#!/bin/sh
set -eu

docker exec -it mongos mongosh -u "${MONGO_ROOT_USERNAME:-clusterAdmin}" -p "${MONGO_ROOT_PASSWORD:-ChangeMeRoot123!}" --authenticationDatabase admin
