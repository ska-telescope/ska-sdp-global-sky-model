#!/usr/bin/env bash

set -e # exit on failure

cd /usr/local/lib/python3.10/dist-packages/ska_sdp_global_sky_model

alembic upgrade head

if [[ "$1" == "--import-sample-data" ]]; then
  python /db_init_data.py /sample_data/*.csv --ignore-import-failure
fi
