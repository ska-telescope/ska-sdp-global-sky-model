Database Guide
~~~~~~~~~~~~~~

This document complements the guidelines set out in the SKA telescope developer
 portal `<https://developer.skao.int/en/latest/>`_

Running the application database
================================

The application expects a PostgreSQL database to be available.
The database needs the following environment variables to be set
correctly:

- POSTGRES_USER: The database user, which should have access to the database
- POSTGRES_PASSWORD: The database's user password.
- POSTGRES_DB_NAME: The database name.
- POSTGRES_HOST: The host that the database is available on.
    (It is vital that the container has access to this host)
- POSTGRES_SCHEMA_NAME: The schema that is available to the user for the user.

Schema management
-----------------

The database schema is being managed by alembic `<https://alembic.sqlalchemy.org/en/latest/>`_

The alembic migration tool is available through the `alembic` command.

The file `alembic/env.py` is generated when initialising the tool,
and has been extended to add functionality adapting to translate schema names as well as
avoiding manual indexes.

Running Migration in Kubernetes
===============================

When the GSM is deployed within a kubernetes environment the database will need
to get the migrations run manually, there are 2 ways of doing this.

Migrating Directly from GSM Container
-------------------------------------

Running a console/shell into a running instance you can run:

.. code-block:: bash

    $ bash /db_migrate.sh

If for some reason you need to downgrade the database (which shouldn't be needed), you can run:

.. code-block:: bash

    $ bash /db_downgrade.sh

Using a Job
-----------

A Job can be used, refer to the `SKA Testing chart <https://gitlab.com/ska-telescope/sdp/ska-sdp-integration/-/blob/master/charts/ska-sdp-testing/templates/gsm-postgres.yaml?ref_type=heads>`_ for an example.

Development Environment
=======================

The development environment can be set up using docker compose or k8s.
In either case it is highly recommended to use the `POSTGRES_SCHEMA_NAME=public`.
This is due to this being the default name used in the migrations.

Updating schema
---------------

The schema is migrated to the latest version by running `alembic migrate`.
This can either be done in the docker compose environment:

.. code-block:: bash

    $ make compose-migrate

Alternatively if this the container is set up using k8s the following command
can be run inside the container:

.. code-block:: bash

    $ make k8s-migrate

The schema changes can be rolled back to a previous version by running `alembic rollback -1`.
This is available in docker compose:

.. code-block:: bash

    $ make compose-migrate-rollback

and in the k8s container by:

.. code-block:: bash

    $ make k8s-migrate-rollback


Schema changes
--------------

The database model may change. In this case these changes need to be consolidated into a
migration file (versions) and added to git.
It is important that the environment is using the public schema in the PostgreSQL schema.

This file may be created manually and added to the folder `alembic/versions/`.
Alternatively this file may be generated. This is done by running in docker compose:

.. code-block:: bash

    $ make compose-create-migration MIGRATION_NOTE='migration_text'

or inside the container

.. code-block:: bash

   $ make k8s-create-migration


