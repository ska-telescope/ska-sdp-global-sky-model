Developer Guide
~~~~~~~~~~~~~~~

This document complements the guidelines set out in the `SKA telescope developer portal <https://developer.skao.int/en/latest/>`_


Development setup
=================

Clone the repository and its submodules:

.. code-block:: bash

    git clone --recursive git@gitlab.com:ska-telescope/sdp/ska-sdp-global-sky-model.git

Service layout
==============

The GSM service consists of two main components:
A FastAPI application, and a PostgreSQL database.
The FastAPI service provides a web interface, as well as a thread that
interfaces with the ConfigurationDB.

To illustrate the components and the various external interfaces the process of requesting an LSM is described:
(Thanks to Jacob, we shamelessly plugged this from https://confluence.skatelescope.org/display/SWSI/Requesting+a+Local+Sky+Model)


.. image:: images/LSM_Flow.png

Application Architecture
========================

The SKA Global Sky Model application consists of several components:

Database Services
-----------------

**PostgreSQL Database**
    Stores the global sky model catalogue data in the following tables:

    - SkyComponent table: Contains all sky component properties including position (RA/Dec), flux measurements,
      spectral indices, polarization data, and morphological parameters
    - GlobalSkyModelMetadata table: Stores catalogue version information and reference frequency

    The schema is dynamically generated from the ``ska-sdp-datamodels`` package to ensure
    consistency with the canonical data model.

**etcd**
    A distributed key-value store used by the SKA SDP configuration system. The application
    uses etcd to:

    - Watch for flow requests (data processing workflows)
    - Coordinate between different SDP services

    The ``request_responder.py`` module starts a background thread that watches etcd for
    flow entries requesting local sky models. When a flow is detected, it processes the
    request and writes the results to the specified location.

API Service
-----------

The FastAPI service provides REST endpoints for:

- Querying components by position and field of view
- Uploading catalogue data
- Managing catalogue versions
- Health checks and status


Running the application
=======================

The service can be run as part of an SDP deployment.
Alternatively, and in a development context, it can also be deployed as a
standalone application.

Running as part of SDP (using Kubernetes)
-----------------------------------------

If the service is deployed as an SDP component, see the SDP Integration
documentation for details:
https://developer.skao.int/projects/ska-sdp-integration/en/latest/installation/standalone.html

Using Docker Compose
--------------------

For an integrated setup, use Docker Compose to run the full application stack. See the
:doc:`Deployment` guide for complete instructions on running with Docker Compose.

Running Standalone (Development)
---------------------------------

The API can also be run as a standalone script for development. This requires:

1. A running PostgreSQL database
2. A running etcd instance (for flow management features)

Then run the API directly:

.. code-block:: bash

    $ uvicorn ska_sdp_global_sky_model.api.app.main:app --reload --host 0.0.0.0 --port 80 --app-dir /usr/src

Running the application tests
=============================

The API is tested using the pytest framework alongside FastAPI's TestClient. The tests can be run with:

.. code-block:: bash

    $ make python-tests

Test Database Setup
-------------------

The tests use an in-memory SQLite database instead of PostgreSQL to avoid the need for a running
database instance during testing.

Database Mocking Strategy
^^^^^^^^^^^^^^^^^^^^^^^^^^

The test suite implements several key mocking strategies:

1. **In-memory SQLite Database**: Tests use ``sqlite:///:memory:`` instead of PostgreSQL, with
   ``StaticPool`` to maintain a single connection across tests.

2. **Q3C Function Mocking**: PostgreSQL's Q3C extension for spherical coordinate searches is
   mocked for SQLite using a simple box check approximation:

   .. code-block:: python

       @event.listens_for(engine, "connect")
       def register_q3c_mock(dbapi_conn, connection_record):
           def q3c_radial_query_mock(ra1, dec1, ra2, dec2, radius):
               # Simple box check - not accurate but sufficient for testing
               ra_diff = abs(ra1 - ra2)
               dec_diff = abs(dec1 - dec2)
               return 1 if (ra_diff <= radius and dec_diff <= radius) else 0
           dbapi_conn.create_function("q3c_radial_query", 5, q3c_radial_query_mock)

3. **JSONB Compatibility**: PostgreSQL's JSONB type is replaced with JSON for SQLite compatibility:

   .. code-block:: python

       @event.listens_for(Base.metadata, "before_create")
       def replace_jsonb_sqlite(target, connection, **kw):
           if connection.dialect.name == "sqlite":
               for table in target.tables.values():
                   table.schema = None
                   for column in table.columns:
                       if isinstance(column.type, JSONB):
                           column.type = JSON()

4. **Startup Function Mocking**: Database connection checks and background threads are mocked to
   prevent actual connections during tests:

   .. code-block:: python

       with patch("ska_sdp_global_sky_model.api.app.main.wait_for_db"), \\
            patch("ska_sdp_global_sky_model.api.app.main.start_thread"), \\
            patch("ska_sdp_global_sky_model.api.app.main.q3c_index"), \\
            patch("ska_sdp_global_sky_model.api.app.main.engine", engine):
           # Test code here

See [test_main.py](tests/test_main.py) for a complete example of the mocking implementation.
