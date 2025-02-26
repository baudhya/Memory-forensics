# Use the official Cassandra image as the base
FROM cassandra:latest

# Copy the initialization CQL script into the container
COPY init-keyspace.cql /docker-entrypoint-initdb.d/

# The default entrypoint of Cassandra will automatically execute scripts in /docker-entrypoint-initdb.d/
