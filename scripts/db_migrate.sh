#!/usr/bin/env bash

set -e # exit on failure

if [[ -z ${BASE_DIRECTORY+x} ]]; then
  BASE_DIRECTORY="/app/ska_sdp_global_sky_model"
fi

if [[ ! -d "$BASE_DIRECTORY" ]]; then
  echo "Directory doesn't exist: BASE_DIRECTORY='$BASE_DIRECTORY'" >&2
  exit 1
fi

cd "$BASE_DIRECTORY"

alembic upgrade head

if [[ "$1" == "--import-sample-data" ]]; then
  python /db_init_data.py --ignore-import-failure --metadata-file /sample_data/metadata_test.json /sample_data/*.csv
fi
