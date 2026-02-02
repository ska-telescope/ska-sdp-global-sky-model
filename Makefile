include .make/base.mk
include .make/oci.mk
include .make/python.mk


PYTHON_LINE_LENGTH = 99
CHANGELOG_FILE = CHANGELOG.rst

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

migrate:
	docker compose exec -w /usr/src/ska_sdp_global_sky_model/ fastapi alembic upgrade head

migrate-rollback:
	docker compose exec -w /usr/src/ska_sdp_global_sky_model/ fastapi "alembic upgrade downgrade -1"

MIGRATION_NOTE = "New Migration Note"
create-migration:
	docker compose exec -w /usr/src/ska_sdp_global_sky_model/ fastapi bash -c "alembic revision  --autogenerate -m $(MIGRATION_NOTE)"
	docker compose exec -w /usr/src/ska_sdp_global_sky_model/ fastapi bash -c "chmod 0666 alembic/versions/*.py"

prod-migrate:
	cd /usr/local/lib/python3.10/dist-packages/ska_sdp_global_sky_model && alembic upgrade head

prod-migrate-rollback:
	cd /usr/local/lib/python3.10/dist-packages/ska_sdp_global_sky_model && alembic upgrade downgrade -1
