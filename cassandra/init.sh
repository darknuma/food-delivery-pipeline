#!/bin/bash
# Wait for Cassandra to be ready
until cqlsh -e "describe keyspaces"; do
  echo "Cassandra is unavailable - sleeping"
  sleep 2
done

echo "Initializing Cassandra database..."
cqlsh -f /docker-entrypoint-initdb.d/scripts/cassandra_setup.cql



