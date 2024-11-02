FROM postgres:latest

# Environment variables to set up the database, user, and password
ENV DB_USER=myuser
ENV DB_PASSWORD=mypassword
ENV DB_NAME=mydatabase
ENV POSTGRES_USER=myuser
ENV POSTGRES_PASSWORD=mypassword
ENV POSTGRES_DB=mydatabase

# Expose PostgreSQL default port
EXPOSE 5432

# Health check for PostgreSQL
HEALTHCHECK --interval=30s --timeout=10s --retries=3 CMD pg_isready -U $POSTGRES_USER || exit 1

# Copy initialization scripts
# COPY init.sql /docker-entrypoint-initdb.d/

# Start the PostgreSQL server
# Copy the modified configuration file
COPY postgresql.conf /usr/share/postgresql/postgresql.conf

# Override CMD to use the custom configuration
CMD ["postgres", "-c", "config_file=/usr/share/postgresql/postgresql.conf"]
