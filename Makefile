include .make/base.mk
include .make/oci.mk
include .make/helm.mk
include .make/python.mk
include .make/tmdata.mk
include .make/k8s.mk


PYTHON_LINE_LENGTH = 99

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

upload-gsm-backup:
	ska-telmodel upload --repo=ska-telescope/sdp/ska-sdp-global-sky-model assets/dump.sql.gz ska/gsm/global_dump.sql.gz

download-gsm-backup:
	ska-telmodel --sources=gitlab://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model?gsm-data#tmdata cp ska/gsm/global_dump.sql.gz assets/dump.sql.gz

make-dev-db: download-gsm-backup decompress-gsm-db restore-gsm-db
