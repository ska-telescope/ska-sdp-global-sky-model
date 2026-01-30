Deployment Guide
~~~~~~~~~~~~~~~~

This document complements the guidelines set out in the SKA telescope developer
 portal `<https://developer.skao.int/en/latest/>`_

This service is deployed in `SDP <https://developer.skao.int/projects/ska-sdp-integration/en/latest/>`_.

Running the application with Docker Compose
===========================================

The application can be built and run using the provided docker-compose file. The setup includes:

- **PostgreSQL database**: Stores the global sky model data
- **FastAPI service**: REST API for accessing the sky model
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

**Interactive API documentation** is available at ``http://localhost:8000/docs``.

When this setup is running, the database schema should be initialised or migrated by running:

.. code-block:: bash

    $ docker compose up -d

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
