FROM postgres:18

# Install build dependencies
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        postgresql-18-q3c \
        ca-certificates \
        postgresql-server-dev-18; \
    rm -rf /var/lib/apt/lists/*


COPY db/*.sql /docker-entrypoint-initdb.d/

EXPOSE 5432

CMD ["postgres"]
