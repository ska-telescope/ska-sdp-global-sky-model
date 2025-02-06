# include OCI Images support
include .make/oci.mk

# include Helm Chart support
include .make/helm.mk

# Include Python support
include .make/python.mk

# include core make support
include .make/base.mk

# include tmdata support
include .make/tmdata.mk

include .make/k8s.mk

PYTHON_LINE_LENGTH = 99

build:
	docker build \
		--target dev \
		--tag ska-sdp-gsm .

run:
	mkdir -p gsm_local_data
	docker run \
		--publish 8000:80 \
		--env API_VERBOSE=true \
		--env DATASET_ROOT=gsm_local_data/ \
		--volume ${PWD}/gsm_local_data:/usr/src/ska_sdp_global_sky_model/datasets \
		--volume ${PWD}/src/ska_sdp_global_sky_model:/usr/src/ska_sdp_global_sky_model \
		ska-sdp-gsm

run-local:
	DATASET_ROOT=gsm_local_data \
		poetry run \
			uvicorn ska_sdp_global_sky_model.api.main:app \
				--reload \
				--host 127.0.01 \
				--port 8000

ingest:
	DATASET_ROOT=gsm_local_data \
		poetry run gsm-ingest

upload-gsm-backup:
	ska-telmodel upload \
		--repo=ska-telescope/sdp/ska-sdp-global-sky-model \
		assets/dump.sql.gz ska/gsm/global_dump.sql.gz

download-gsm-backup:
	ska-telmodel \
		--sources=gitlab://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model?gsm-data#tmdata \
		cp \
			ska/gsm/global_dump.sql.gz \
			assets/dump.sql.gz
