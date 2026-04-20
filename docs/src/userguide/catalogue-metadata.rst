.. _catalogue_metadata:

Querying catalogue metadata
---------------------------

The Global Sky Model service stores metadata describing each catalogue
that has been uploaded to the system. This metadata includes the
catalogue name, version, description and upload information.

The metadata can be queried using the ``/catalogue-metadata`` endpoint.
This endpoint supports filtering, sorting and selecting specific
columns through query parameters. The shared syntax is documented in
:ref:`querying_data`.

Retrieving catalogue metadata
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To retrieve catalogue metadata records, navigate to:

``GET /catalogue-metadata``

By default this returns all catalogue metadata entries currently stored
in the database.

Example request:

.. code-block::

    GET /catalogue-metadata

Example response:

.. code-block:: json

    [
        {
            "id": 1,
            "catalogue_name": "GLEAM",
            "version": "1.0.0",
            "description": "GLEAM survey catalogue",
            "upload_id": "upload_123",
            "staging": false
        },
        {
            "id": 2,
            "catalogue_name": "LOFAR",
            "version": "2.0.0",
            "description": "LOFAR sky catalogue",
            "upload_id": "upload_456",
            "staging": false
        }
    ]

Filtering results
^^^^^^^^^^^^^^^^^

Use the generic query syntax described in :ref:`querying_data`.

Example:

.. code-block:: text

    GET /catalogue-metadata?version__gt=1.0

This returns catalogues with versions greater than ``1.0``.

To request a bounded range, combine minimum and maximum filters:

.. code-block:: text

    GET /catalogue-metadata?version__gte=2.0&version__lte=3.0

This returns catalogue metadata records whose version falls between ``2.0`` and
``3.0`` inclusive.

Sorting results
^^^^^^^^^^^^^^^

Results can be sorted using the ``sort`` query parameter.

Multiple fields may be specified as a comma-separated list.
Prefix a field with ``-`` to sort in descending order.

Examples:

.. code-block:: text

    GET /catalogue-metadata?sort=version

Sort results by version ascending.

.. code-block:: text

    GET /catalogue-metadata?sort=-version

Sort results by version descending.

Selecting specific fields
^^^^^^^^^^^^^^^^^^^^^^^^^

To limit the returned columns, use the ``fields`` query parameter.

This accepts a comma-separated list of column names.

Example:

.. code-block:: text

    GET /catalogue-metadata?fields=version,catalogue_name

Example response:

.. code-block:: json

    [
        {
            "version": "1.0.0",
            "catalogue_name": "GLEAM"
        },
        {
            "version": "2.0.0",
            "catalogue_name": "LOFAR"
        }
    ]

Limiting results
^^^^^^^^^^^^^^^^

The number of returned records can be limited using the ``limit`` parameter.

Example:

.. code-block:: text

    GET /catalogue-metadata?limit=10

This returns at most 10 catalogue metadata records.

Notes
^^^^^

1. Any query parameters that do not correspond to valid metadata columns
   are ignored.
2. Filtering operators are applied before sorting.
3. Field selection is applied after the query results are retrieved.
4. The returned objects correspond directly to metadata entries stored
   in the Global Sky Model database.
