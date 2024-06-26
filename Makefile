# include OCI Images support
include .make/oci.mk

# include Helm Chart support
include .make/helm.mk

# Include Python support
include .make/python.mk

# include core make support
include .make/base.mk

PYTHON_LINE_LENGTH = 99

build:
	docker compose pull
	docker compose build

run:
	docker compose up -d