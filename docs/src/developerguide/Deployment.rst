Deployment Guide
~~~~~~~~~~~~~~~~

This document complements the guidelines set out in the SKA telescope developer
 portal `<https://developer.skao.int/en/latest/>`_

Kubernetes Deployment
=====================

The SKA Global Sky Model is designed to be deployed as a service, accesible to
other deployments via the included API.

Steps to run the system locally in Minikube
===========================================

The following steps assume that you have cloned the repo, or have a local 
copy of the chart. All given commands assume that you are at the terminal in
your chosen environment.

1. Start Minikube if it is not already running, and optionally enable the 
ingress addon: 

.. code-block:: bash

    $ minikube start
    $ minikube addons enable ingress

2. Change to the chart directory in the repository: ``cd charts/ska-sdp-global-sky-model``.
Make any desired changes to the values files. A local values file can be specified for easy
management of values used for dev purposes.

3. Optionally, create a new namespace: ``kubectl create namespace [namespace]``.

4. Install the helm chart as follows:

.. code-block:: bash

    $ helm install [deployment-name] charts/ska-sdp-global-sky-model -n [namespace] --values values_local_deployment.yaml

Once the install has completed, you will have the following running:

* The Global Sky Model PostgreSQL database.
* The Global Sky Model FastAPI 

Running ``minikube service`` will return a URL to connect to the database via the API.

Running the application with Docker Compose
===========================================

The application can be built and run using the provided docker-compose file. The setup includes:

- **PostgreSQL database**: Stores the global sky model data
- **FastAPI service**: REST API for accessing the sky model
- **Redis**: Session management and caching
- **etcd**: Configuration store for SKA SDP flow management

Prerequisites
-------------

Install Docker and docker-compose:

.. code-block:: bash

    $ sudo apt install docker.io docker-compose

Starting the Services
---------------------

To start all services:

.. code-block:: bash

    $ docker compose up -d

This will start all required services in the background. The API will be available at ``http://localhost:8000``.

Service Configuration
---------------------

The docker-compose setup includes the following services:

**etcd Service**
    Provides distributed configuration management for SKA SDP workflows. The FastAPI service 
    connects to etcd using environment variables:
    
    - ``SDP_CONFIG_HOST``: Hostname of the etcd service (default: ``etcd``)
    - ``SDP_CONFIG_PORT``: Port of the etcd service (default: ``2379``)

**PostgreSQL Database**
    - Port: ``5432``
    - Database: ``postgres``
    - Password: ``pass`` (configurable via ``POSTGRES_PASSWORD``)

**FastAPI Service**
    - Port: ``8000`` (mapped to container port 80)
    - Mounts: ``./src/ska_sdp_global_sky_model`` for live code reloading during development

**Redis**
    - Port: ``6379``

Stopping the Services
---------------------

To stop all services:

.. code-block:: bash

    $ docker compose down

Viewing Logs
------------

To view logs from all services:

.. code-block:: bash

    $ docker compose logs -f
    # Or for a specific service
    $ docker compose logs -f fastapi
