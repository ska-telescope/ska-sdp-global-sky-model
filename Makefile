include .make/base.mk
include .make/oci.mk
include .make/helm.mk
include .make/python.mk
include .make/k8s.mk


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


GSM_VERSION ?= 0.0.1
make sql-schema:
	$(PYTHON_VARS_BEFORE_PYTEST) python gsm_schema/gsm_schema.py > ./gsm_schema_${GSM_VERSION}.sql
