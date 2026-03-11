include .make/base.mk
include .make/oci.mk
include .make/python.mk


PYTHON_LINE_LENGTH = 99
CHANGELOG_FILE = CHANGELOG.rst
MIGRATION_NOTE = New Migration Note

build:
	docker compose pull
	docker compose build

run:
	docker compose up -d

backup-gsm-db:
	mkdir -p assets
	docker compose exec db pg_dump postgres > assets/dump.sql

compress-gsm-db:
	gzip -9 assets/dump.sql

decompress-gsm-db:
	gunzip assets/dump.sql.gz -f

restore-gsm-db:
	sleep 1
	docker compose exec -T db psql postgres < assets/dump.sql

compose-migrate:
	docker compose exec -w /usr/src/ska_sdp_global_sky_model/ fastapi alembic upgrade head

compose-migrate-rollback:
	docker compose exec -w /usr/src/ska_sdp_global_sky_model/ fastapi alembic downgrade -1

compose-create-migration:
	docker compose exec -w /usr/src/ska_sdp_global_sky_model/ fastapi bash -c "alembic revision  --autogenerate -m '$(MIGRATION_NOTE)'"
	docker compose exec -w /usr/src/ska_sdp_global_sky_model/ fastapi bash -c "chmod 0666 alembic/versions/*.py"

generate-schema:
	poetry run python scripts/generate_db_schema.py

local-migrate:
	poetry run bash -c "cd src/ska_sdp_global_sky_model; POSTGRES_HOST=localhost alembic upgrade head"

local-create-migration:
	poetry run bash -c "cd src/ska_sdp_global_sky_model; POSTGRES_HOST=localhost alembic revision  --autogenerate -m '$(MIGRATION_NOTE)'"

local-sample:
	POSTGRES_HOST=localhost poetry run python scripts/db_init_data.py \
		--metadata-file 'tests/data/metadata_generic_1.5.0.json' \
		'tests/data/test_catalogue_1.csv' \
		'tests/data/test_catalogue_2.csv'
