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

Running the application
=======================

The API can be run as a script, provided the connection string to the PostgreSQL database 
is updated, using the command:

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

Updating the schema
===================

After updating the schema (models.py) a migration needs to be created. These can either be created manually
or by auto-generating. Auto generating these is the recommended path. This can be done by running the following
command:

.. code-block:: bash

    $ make create-migration


The migration files are stored in src/ska_sdp_global_sky_model/alembic/versions. These should be added to the
repository. Applying the migrations can be done by running the command:

.. code-block:: bash

    $ make migrate
