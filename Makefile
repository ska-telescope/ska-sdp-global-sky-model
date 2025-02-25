include .make/base.mk
include .make/oci.mk
include .make/helm.mk
include .make/python.mk
include .make/tmdata.mk
include .make/k8s.mk

PYTHON_LINE_LENGTH = 99

OPTS ?= --verbose racs
GSM_DATA ?= datasets

GSM_VERSION ?= $(shell date "+%Y%m%d")

build:
	docker build \
		--target dev \
		--tag ska-sdp-gsm .

run:
	mkdir -p ${GSM_DATA}
	docker run \
		--publish 8000:80 \
		--env API_VERBOSE=true \
		--env DATASET_ROOT=/gsm/datasets \
		--volume ${PWD}/${GSM_DATA}:/gsm/datasets \
		--volume ${PWD}/src/ska_sdp_global_sky_model:/usr/src/ska_sdp_global_sky_model \
		ska-sdp-gsm

run-local:
	DATASET_ROOT=${GSM_DATA} \
	TMDATA_SOURCE='file://tmdata/' \
	TMDATA_KEYS='ska/sdp/gsm/ASKAP_20250206.tar.gz,ska/sdp/gsm/Murchison_Widefield_Array_20250218.tar.gz' \
		poetry run \
			uvicorn ska_sdp_global_sky_model.api.main:app \
				--reload \
				--host 127.0.01 \
				--port 8000

ingest:
	DATASET_ROOT=${GSM_DATA} \
		poetry run gsm-ingest ${OPTS}


upload-gsm-askap:
	cd ${GSM_DATA}; tar cf - "ASKAP" | pigz -9 > "${PWD}/ASKAP_${GSM_VERSION}.tar.gz"
	ska-telmodel upload \
		--force-car-upload \
		--repo=ska-telescope/sdp/ska-sdp-global-sky-model \
		ASKAP_${GSM_VERSION}.tar.gz \
		ska/sdp/gsm/ASKAP_${GSM_VERSION}.tar.gz

upload-gsm-murchison-widefield-array:
	cd ${GSM_DATA}; tar cf - "Murchison Widefield Array" | pigz -9 > "${PWD}/Murchison_Widefield_Array_${GSM_VERSION}.tar.gz"
	ska-telmodel upload \
		--force-car-upload \
		--repo=ska-telescope/sdp/ska-sdp-global-sky-model \
		Murchison_Widefield_Array_${GSM_VERSION}.tar.gz \
		ska/sdp/gsm/Murchison_Widefield_Array_${GSM_VERSION}.tar.gz

compress: upload-gsm-askap upload-gsm-murchison-widefield-array

manual-download:
	TMDATA_SOURCE=car:sdp/ska-sdp-global-sky-model?0.2.0 \
		poetry run gsm-download \
			--verbose \
			ska/sdp/gsm/ASKAP_20250206.tar.gz \
			ska/sdp/gsm/Murchison_Widefield_Array_20250218.tar.gz