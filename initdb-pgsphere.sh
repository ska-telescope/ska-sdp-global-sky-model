#!/bin/sh 

set -e 

export PGUSER="$POSTGRES_USER"

"${psql[@]}" <<- 'EOSQL'
CREATE DATABASE test IS_TEMPLATE true;
EOSQL
