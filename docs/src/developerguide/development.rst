Developer Guide
===============

This document complements the guidelines set out in the
`SKA telescope developer portal <https://developer.skao.int/en/latest/>`_.

Clone the repository and its submodules:

.. code-block:: bash

    git clone --recursive git@gitlab.com:ska-telescope/sdp/ska-sdp-global-sky-model.git

The repository is integrated with the `ska-cicd-makefile <https://gitlab.com/ska-telescope/sdi/ska-cicd-makefile>`_
submodule, which provides standardised ``make`` commands, for example, for linting and testing.

The GSM code is based on `FastAPI <https://fastapi.tiangolo.com/>`_ and `SQLAlchemy <https://www.sqlalchemy.org/>`_.

It connects to two database backends to provide its full functionality:

- **PostgreSQL Database**

  Stores the global sky model catalogue data in the following tables:

  - ``SkyComponent`` table: Contains all sky component properties
  - ``GlobalSkyModelMetadata`` table: Stores catalogue version information and reference frequency

  The schema is dynamically generated from the ``ska-sdp-datamodels`` package to ensure
  consistency with the canonical data model. See :ref:`db_design`.

- **etcd**

  A distributed key-value store used by the SKA SDP Configuration database.
  The application uses ``etcd`` to watch for flow requests (data processing workflows)
  (see :ref:`sdp_request`).

  The ``request_responder.py`` module starts a background thread that watches etcd for
  flow entries requesting local sky models. When a flow is detected, it processes the
  request and writes the results to the specified location.

The FastAPI service provides REST endpoints for:

- Querying the Global Sky Model database (see :ref:`lsm_browser`)
- Uploding new catalogue data (see :ref:`upload_api`)
- Monitoring the upload process (see :ref:`upload_api`)

Running the application tests
-----------------------------

The API is tested using the pytest framework alongside FastAPI's TestClient. The tests can be run with:

.. code-block:: bash

    $ make python-tests

Test database setup
^^^^^^^^^^^^^^^^^^^

The tests use an in-memory SQLite database instead of PostgreSQL to avoid the need for a running
database instance during testing.

Database Mocking Strategy
.........................

The test suite implements several key mocking strategies:

1. In-memory SQLite database: Tests use ``sqlite:///:memory:`` instead of PostgreSQL, with
   ``StaticPool`` to maintain a single connection across tests.

2. Q3C function mocking: PostgreSQL's Q3C extension for spherical coordinate searches is
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

3. JSONB compatibility: PostgreSQL's JSONB type is replaced with JSON for SQLite compatibility:

   .. code-block:: python

       @event.listens_for(Base.metadata, "before_create")
       def replace_jsonb_sqlite(target, connection, **kw):
           if connection.dialect.name == "sqlite":
               for table in target.tables.values():
                   table.schema = None
                   for column in table.columns:
                       if isinstance(column.type, JSONB):
                           column.type = JSON()

4. Startup function mocking: Database connection checks and background threads are mocked to
   prevent actual connections during tests:

   .. code-block:: python

       with patch("ska_sdp_global_sky_model.api.app.main.wait_for_db"), \\
            patch("ska_sdp_global_sky_model.api.app.main.start_thread"), \\
            patch("ska_sdp_global_sky_model.api.app.main.q3c_index"), \\
            patch("ska_sdp_global_sky_model.api.app.main.engine", engine):
           # Test code here
