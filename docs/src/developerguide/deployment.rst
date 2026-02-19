Deployment Guide
================

The GSM service is deployed as an SDP component (including its required database
backend), which allows full integration testing of the code. Follow the
`SDP documentation <https://developer.skao.int/projects/ska-sdp-integration/en/latest/installation/standalone.html>`_
for guidelines.

Running the application locally
-------------------------------

The application can be built and run locally, with the backend services
deployed using docker or podman.

Starting the database services
..............................

Running an ``etcd`` instance:

.. code-block::

    docker run -it  \
      -p 2379:2379 \
      -e ETCD_NAME=ska-sdp-etcd-0 \
      -e ETCD_LISTEN_CLIENT_URLS=http://0.0.0.0:2379 \
      -e ETCD_ADVERTISE_CLIENT_URLS=http://0.0.0.0:2379 \
      -e ETCD_MAX_TXN_OPS=1024 \
      artefact.skao.int/ska-sdp-etcd:3.5.21 \
      etcd

The repository contains two `Dockerfiles <https://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model/-/tree/main/images>`_,
one for creating a local PostgreSQL database and another for the GSM Service.

Building the PostgreSQL DB:

.. code-block::

    docker build -t gsm-db -f images/ska-sdp-global-sky-model-db/Dockerfile .

Running the PostgreSQL DB:

.. code-block::

    podman run -d \
      --mount source=gsm_db_vol,target=/var/lib/postgresql/data \
      -e POSTGRES_PASSWORD=pass \
      -e POSTGRES_DB=postgres \
      -p 5432:5432 \
      gsm-db

Starting the GSM Service using docker
.....................................

Using the GSM Dockerfile, build an image:

.. code-block::

    docker build -t gsm -f images/ska-sdp-global-sky-model-api/Dockerfile .

Start the service:

.. code-block::

    podman run -d \
      -e API_VERBOSE=true \
      -e POSTGRES_HOST=localhost \
      -e POSTGRES_PORT=5432 \
      -e POSTGRES_DB=postgres \
      -e POSTGRES_USER=postgres \
      -e POSTGRES_PASSWORD=pass \
      -p 8000:80
      gsm

This will start the GSM service in the background.
The API will be available at ``http://localhost:8000``.

Running the GSM service as a script
...................................

The API can also be run as a standalone script for development. This requires:

1. A running PostgreSQL database
2. A running etcd instance (for flow management features)

You can achieve this using docker, or by installing local versions of the services.

Then run the API directly:

.. code-block:: bash

    $ uvicorn ska_sdp_global_sky_model.api.app.main:app --reload --host 0.0.0.0 --port 80 --app-dir /usr/src

The API will be available at ``http://localhost:8000``.
To access the API documentation, navigate to ``http://localhost:8000/docs``
