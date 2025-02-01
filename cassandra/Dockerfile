FROM cassandra:latest

COPY ./scripts/cassandra_setup.cql /docker-entrypoint-initdb.d/scripts/cassandra_setup.cql

# Create an initialization script
COPY ./init.sh /docker-entrypoint-initdb.d/
RUN chmod +x /docker-entrypoint-initdb.d/init.sh