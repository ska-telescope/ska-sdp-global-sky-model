.. _db_design:

PostgreSQL Database
-------------------

The GSM service uses a PostgreSQL database to store sky model data, catalogue metadata,
and versioning information. The database schema is designed to support efficient querying
of sky model components based on position and field of view, as well as to manage multiple
versions of the global sky model.

The PostgreSQL database is a separate service to the GSM, which the GSM connects
to via `SQLAlchemy <https://www.sqlalchemy.org/>`_.
The database schema is managed by `Alembic <https://alembic.sqlalchemy.org/en/latest/>`_.

Database schema
^^^^^^^^^^^^^^^

The schema for the database tables is generated from an agreed datamodel (see :ref:<>), currently stored in the
`ska-sdp-datamodels package <https://gitlab.com/ska-telescope/sdp/ska-sdp-datamodels/-/blob/main/src/ska_sdp_datamodels/global_sky_model/global_sky_model.py?ref_type=heads>`_.
The columns and data types are defined in these models, which allow sharing the structure with
users as well.

When the data models are updated, the database schema is migrated to a new version.
This is a manual process and can be performed using the tools provided by the GSM
(using ``make``, see :ref:``).

