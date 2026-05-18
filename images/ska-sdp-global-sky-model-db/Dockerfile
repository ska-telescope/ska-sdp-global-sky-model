FROM postgres:18

# Install build dependencies
RUN set -eux; \
    apt update; \
    apt install -y --no-install-recommends \
        postgresql-18-q3c \
        ca-certificates \
        postgresql-server-dev-18; \
    apt -y upgrade; \
    rm -rf /var/lib/apt/lists/*


COPY db/*.sql /docker-entrypoint-initdb.d/

EXPOSE 5432

CMD ["postgres"]
