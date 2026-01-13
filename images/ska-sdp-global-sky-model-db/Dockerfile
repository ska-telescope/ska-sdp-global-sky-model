FROM postgres:16

# Install build dependencies
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        postgresql-16-q3c \
        ca-certificates \
        postgresql-server-dev-16; \
    rm -rf /var/lib/apt/lists/*


COPY db/*.sql /docker-entrypoint-initdb.d/

EXPOSE 5432

CMD ["postgres"]