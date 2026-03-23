include .make/base.mk
include .make/oci.mk
include .make/python.mk


PYTHON_LINE_LENGTH = 99
CHANGELOG_FILE = CHANGELOG.rst
MIGRATION_NOTE ?= New Migration Note

RUN_LOCATION ?= local
SDP_NAMESPACE ?=
GSM_POD ?=
DOCKER ?= docker compose

## TARGET: run-docker
## SYNOPSIS: make run-docker
## HOOKS: none
## VARS: none
##
## Run the entire environment using Docker

run-docker:  ## Run GSM in docker
	${DOCKER} pull etcd
	${DOCKER} build fastapi db
	${DOCKER} up -d

## TARGET: run
## SYNOPSIS: make run
## HOOKS: none
## VARS: none
##
## Run the GSM by using poetry directly on your machine

run:  ## Run GSM directly in poetry (requires etcd and DB to be up)
	poetry run uvicorn ska_sdp_global_sky_model.api.app.main:app --reload

## TARGET: migrate
## SYNOPSIS: make migrate
## HOOKS: none
## VARS:
##       RUN_LOCATION=<local|docker|kubernetes default=local>
##       SDP_NAMESPACE=<namespace where GSM is running>
##       GSM_POD=<pod name of the running GSM instance>
##
## Run a migration on a running instance of the GSM.
##
## Commonly one of the following is done:
## - `make migrate` - will run migrations on local setup
## - `make migrate RUN_LOCATION=docker` - will run migrations on the running instance in docker
## - `make migrate RUN_LOCATION=kubernetes SDP_NAMESPACE=my_namespace GSM_POD=ska-sdp-gsm-aaaa` - will run migrations on a running instance in kubernetes

migrate:  ## Run the database migration
	case "${RUN_LOCATION}" in \
		local|poetry|python) BASE_DIRECTORY="src/ska_sdp_global_sky_model" poetry run bash scripts/db_migrate.sh ;; \
		docker|compose) ${DOCKER} exec -e BASE_DIRECTORY="/usr/src/ska_sdp_global_sky_model/" fastapi bash /db_migrate.sh ;; \
		k8s|kubernetes|helm) kubectl --namespace "${SDP_NAMESPACE}" exec "${GSM_POD}" -- bash /db_migrate.sh ;; \
		*) echo "unsupported environment";; \
	esac

## TARGET: migrate-create
## SYNOPSIS: make migrate-create
## HOOKS: none
## VARS:
##       RUN_LOCATION=<local|docker>
##       MIGRATION_NOTE="<descriptiong of migration>"
##
## Create any needed migrations for the running GSM instance.
##
## Commonly one of the following is done:
## - `make migrate MIGRATION_NOTE="upgrade schema"` - will create migrations on local setup
## - `make migrate RUN_LOCATION=docker MIGRATION_NOTE="upgrade schema"` - will create migrations on the running instance in docker

migrate-create: ## Create new migrations
	case "${RUN_LOCATION}" in \
		local|poetry|python) poetry run bash -c "cd src/ska_sdp_global_sky_model; alembic revision  --autogenerate -m '${MIGRATION_NOTE}'" ;; \
		docker|compose) ${DOCKER} exec -w /usr/src/ska_sdp_global_sky_model/ fastapi alembic revision  --autogenerate -m '${MIGRATION_NOTE}' ;; \
		*) echo "unsupported environment";; \
	esac

## TARGET: migrate-rollback
## SYNOPSIS: make migrate-rollback
## HOOKS: none
## VARS:
##       RUN_LOCATION=<local|docker|kubernetes default=local>
##       SDP_NAMESPACE=<namespace where GSM is running>
##       GSM_POD=<pod name of the running GSM instance>
##
## Rollback 1 migration on a running instance of the GSM.
##
## Commonly one of the following is done:
## - `make migrate-rollback` - will rollback 1 migration on local setup
## - `make migrate-rollback RUN_LOCATION=docker` - will rollback 1 migration on the running instance in docker
## - `make migrate-rollback RUN_LOCATION=kubernetes SDP_NAMESPACE=my_namespace GSM_POD=ska-sdp-gsm-aaaa` - will rollback 1 migration on a running instance in kubernetes

migrate-rollback:  ## Rollback 1 migration
	case "${RUN_LOCATION}" in \
		local|poetry|python) BASE_DIRECTORY="src/ska_sdp_global_sky_model" poetry run bash scripts/db_downgrade.sh ;; \
		docker|compose) ${DOCKER} exec -e BASE_DIRECTORY="/usr/src/ska_sdp_global_sky_model/" fastapi bash /db_downgrade.sh ;; \
		k8s|kubernetes|helm) kubectl --namespace "${SDP_NAMESPACE}" exec "${GSM_POD}" -- bash /db_downgrade.sh ;; \
		*) echo "unsupported environment";; \
	esac

## TARGET: sample
## SYNOPSIS: make sample
## HOOKS: none
## VARS:
##       RUN_LOCATION=<local|docker|kubernetes default=local>
##       SDP_NAMESPACE=<namespace where GSM is running>
##       GSM_POD=<pod name of the running GSM instance>
##
## Create the sample data (or create a new version if already existing)
##
## Commonly one of the following is done:
## - `make sample` - create a sample GSM using the local setup
## - `make sample RUN_LOCATION=docker` - create a sample GSM using an instance in docker
## - `make sample RUN_LOCATION=kubernetes SDP_NAMESPACE=my_namespace GSM_POD=ska-sdp-gsm-aaaa` - create a sample GSM using an instance in kubernetes

sample:  ## Setup database using a sample dataset
	case "${RUN_LOCATION}" in \
		local|poetry|python) \
			poetry run python scripts/db_init_data.py \
				--metadata-file 'tests/data/metadata_generic_1.5.0.json' \
				'tests/data/test_catalogue_1.csv' \
				'tests/data/test_catalogue_2.csv' ;; \
		docker|compose) \
			${DOCKER} exec fastapi python /db_init_data.py \
				--metadata-file '/sample_data/metadata_generic_1.5.0.json' \
				'/sample_data/test_catalogue_1.csv' \
				'/sample_data/test_catalogue_2.csv' ;; \
		k8s|kubernetes|helm) \
			kubectl --namespace "${SDP_NAMESPACE}" exec "${GSM_POD}" -- python /db_init_data.py \
				--metadata-file '/sample_data/metadata_generic_1.5.0.json' \
				'/sample_data/test_catalogue_1.csv' \
				'/sample_data/test_catalogue_2.csv' ;; \
		*) echo "unsupported environment" ;; \
	esac

