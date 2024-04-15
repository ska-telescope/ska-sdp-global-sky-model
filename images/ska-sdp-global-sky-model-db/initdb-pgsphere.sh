#!/bin/sh

set -e

export PGUSER="$POSTGRES_USER"

"${psql[@]}" <<- 'EOSQL'
CREATE DATABASE template_pgsphere IS_TEMPLATE true;
EOSQL

for DB in template_pgsphere "$POSTGRES_DB"; do
        echo "Loading pg-sphere extension into $DB"
        "${psql[@]}" --dbname="$DB" <<-'EOSQL'
                CREATE EXTENSION IF NOT EXISTS pg_sphere
EOSQL
done