Developer Guide
~~~~~~~~~~~~~~~

This document complements the guidelines set out in the `SKA telescope developer portal <https://developer.skao.int/en/latest/>`_


Tooling Pre-requisites
======================

Below are some tools that will be required to work with the SKA Global Sky Model:

- Python 3.10 or later versions: Install page URL: https://www.python.org/downloads/
- Poetry 1.8.2 or later versions: Install page URL: https://python-poetry.org/docs/#installation
- GNU make 4.2 or later versions: Install page URL: https://www.gnu.org/software/make/


Development setup
=================

Clone the repository and its submodules:

.. code-block:: bash

    git clone --recursive git@gitlab.com:ska-telescope/sdp/ska-sdp-global-sky-model.git

Application Architecture
========================

The SKA Global Sky Model application consists of several components:

Database Services
-----------------

**PostgreSQL Database**
    Stores the global sky model catalog data in the following tables:
    
    - Source table: Contains all source properties including position (RA/Dec), flux measurements,
      spectral indices, polarization data, and morphological parameters
    - GlobalSkyModelMetadata table: Stores catalog version information and reference frequency
    
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

- Querying sources by position and field of view
- Uploading catalog data
- Managing catalog versions
- Health checks and status

Running the application
=======================

Using Docker Compose (Recommended)
-----------------------------------

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

Database Schema Management
==========================

The database schema is dynamically generated from the dataclasses defined in the
``ska-sdp-datamodels`` package. This ensures that the database schema stays in sync
with the canonical data model definitions.

Generating the Schema
---------------------

The database schema is automatically generated by the ``scripts/generate_db_schema.py``
script, which introspects the dataclasses in ``ska_sdp_datamodels.global_sky_model``
and generates SQLAlchemy models with appropriate column types and relationships.

To regenerate the schema after updating the datamodel:

.. code-block:: bash

    $ make generate-schema

This command will:

1. Import the dataclasses from ``ska_sdp_datamodels.global_sky_model.global_sky_model``
2. Analyze field types and infer relationships
3. Generate SQLAlchemy model classes in ``src/ska_sdp_global_sky_model/api/app/db_schema.py``
4. Add appropriate column mappings, foreign keys, and helper methods

The generated ``db_schema.py`` file is committed to the repository and should not be
manually edited. Always regenerate it using the ``make generate-schema`` command.

Schema Configuration
~~~~~~~~~~~~~~~~~~~~

The schema generator is configured using ``scripts/db_config.py``, which separates 
database-specific settings from the core generation logic. This allows you to:

- Map dataclass field names to database column names (``COLUMN_NAME_OVERRIDES``)
- Map dataclass names to table names (``TABLE_NAME_OVERRIDES``)
- Add additional database-specific fields (``DB_SPECIFIC_FIELDS``)
- Add unique constraints (``UNIQUE_FIELDS``)
- Configure custom JSON serialization (``CUSTOM_JSON_SERIALIZATION``)
- Skip certain models from generation (``SKIP_MODELS``)

Example configuration from ``scripts/db_config.py``:

.. code-block:: python

    DB_SPECIFIC_FIELDS = {
        "SkySource": {
            "healpix_index": {
                "type": "BigInteger",
                "kwargs": {"index": True, "nullable": False},
                "description": "HEALPix position for spatial indexing",
            },
        }
    }

    UNIQUE_FIELDS = {
        "SkySource": {"name"},
    }

Creating and Applying Migrations
---------------------------------

After updating the schema (by regenerating db_schema.py or modifying models),
a migration needs to be created. Auto-generating these is the recommended path.
This can be done by running the following command:

.. code-block:: bash

    $ make create-migration


The migration files are stored in src/ska_sdp_global_sky_model/alembic/versions. These should be added to the
repository. Applying the migrations can be done by running the command:

.. code-block:: bash

    $ make migrate
