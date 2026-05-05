Database Guide
==============

Running the application database
--------------------------------

The application expects a PostgreSQL database to be available.
The database needs the following environment variables to be set
correctly:

- POSTGRES_USER: The database user, which should have access to the database
- POSTGRES_PASSWORD: The database's user password.
- POSTGRES_DB_NAME: The database name.
- POSTGRES_HOST: The host that the database is available on. (It is vital that the container has access to this host)
- POSTGRES_SCHEMA_NAME: The schema that is available to the user for the user.

Database schema management
--------------------------

The ``SkyComponent`` model dynamically generates columns from the dataclasses
defined in ``ska-sdp-datamodels``, while database-specific concerns (indexes, methods)
are hardcoded for maintainability.

Schema architecture
...................

The ``models.py`` file defines two models:

**SkyComponent Model**
    - Dynamically generated columns: Field names and types are read from the
      ``SkyComponent`` dataclass at module import time, ensuring automatic synchronization
      with upstream data model changes
    - Hardcoded database fields: The ``gsm_id`` field is explicitly defined for
      cross-table indexing and is not part of the scientific data model

**GlobalSkyModelMetadata Model**
    - Dynamically generated columns: Field names and types are read from the
      ``GlobalSkyModelMetadata`` dataclass at module import time

Dynamic column generation
.........................

The dynamic column generation happens at module import time through the
``_add_dynamic_columns_to_model()`` helper function:

1. The function iterates over the dataclass's ``__annotations__`` dictionary
2. Each field's type annotation is converted to an appropriate SQLAlchemy column type
3. Columns are added as attributes to the model class using ``setattr()``
4. Special handling ensures the ``name`` field is unique and not nullable

This approach runs once when the module is first imported. Subsequent imports use the
cached module with fully-formed classes. When the upstream dataclass changes (e.g.,
new field added), the database model automatically includes that field on the next import—no
code generation needed.

.. _alembic_upload:

Schema management with Alembic
..............................

The database schema is being managed by `Alembic <https://alembic.sqlalchemy.org/en/latest/>`_

The alembic migration tool is available through the ``alembic`` command.

The file ``alembic/env.py`` is generated when initialising the tool,
and has been extended to add functionality adapting to translate schema names as well as
avoiding manual indexes.

Updating schema
~~~~~~~~~~~~~~~

The schema is migrated to the latest version by running ``alembic migrate``.

There are 2 helper commands to do this:

* ``make migrate`` - will migrate the database forwards
* ``make migrate-rollback`` - will rollback a single migration

For both commands there are some variables that can be used:

* ``RUN_LOCATION`` which can be ``local``, ``docker`` or ``kubernetes`` which will run
  on one of the 3 environments.

Note that for the ``kubernetes`` commands you also need:

* ``SDP_NAMESPACE`` - the namespace in which the GSM is deployed.
* ``GSM_POD`` - the name of the GSM pod to use (use the API not the database)

.. code-block:: bash
   :caption: Migrate a local instance

    make migrate

.. code-block:: bash
   :caption: Migrate an instance using docker

    make migrate RUN_LOCATION=docker

.. code-block:: bash
   :caption: Migrate a kubernetes instance

    KUBECONFIG=kubeconfig make migrate RUN_LOCATION=kubernetes SDP_NAMESPACE=dp-phoenix GSM_POD=ska-sdp-gsm-7954755b4f-vjxxz

Schema changes
~~~~~~~~~~~~~~

The database model may change. In this case these changes need to be consolidated into a
migration file (versions) and added to git.
It is important that the environment is using the public schema in the PostgreSQL schema.

This file may be created manually and added to the folder ``alembic/versions/``.
Alternatively this file may be generated. This is done by running a helper command:

.. code-block:: bash
   :caption: Create migration on local setup

    make migrate-create MIGRATION_NOTE="upgrading schema"

.. code-block:: bash
   :caption: Create migration on a docker instance

    make migrate-create RUN_LOCATION=docker MIGRATION_NOTE="upgrading schema"


.. note::

    Migrations cannot be created in kubernetes, as this would cause them to not
    be persisted in the repo.

Running migration in Kubernetes
--------------------------------

When the GSM is deployed within a kubernetes environment the database will need
to get the migrations run manually, there are 2 ways of doing this.

Migrating directly from GSM container
.....................................

Running a console/shell into a running instance you can run:

.. code-block:: bash

    $ bash /db_migrate.sh

If you would like to also have a sample dataset then run:

.. code-block:: bash

    $ bash /db_migrate.sh --import-sample-data

If for some reason you need to downgrade the database (which shouldn't be needed), you can run:

.. code-block:: bash

    $ bash /db_downgrade.sh
