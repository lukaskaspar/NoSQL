#!/bin/sh
docker compose stop cfg1 cfg2 cfg3 \
  shard1a shard1b shard1c \
  shard2a shard2b shard2c \
  shard3a shard3b shard3c \
  mongos compass-web