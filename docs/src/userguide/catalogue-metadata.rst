.. _catalogue_metadata:

Querying catalogue metadata
---------------------------

The Global Sky Model service stores metadata describing each catalogue
that has been uploaded to the system. This metadata includes the
catalogue name, version, description and upload information.

The metadata can be queried using the ``/catalogue-metadata`` endpoint.
This endpoint supports filtering, sorting and selecting specific
columns through query parameters.

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


.. _filter_operators:

Filtering results
^^^^^^^^^^^^^^^^^

Results can be filtered using query parameters corresponding to
database column names. Several operators are supported by appending
a suffix to the parameter name, after a double-underscore.

Supported operators include:

.. list-table:: Supported filter operators
   :widths: 20 50 30
   :header-rows: 1

   * - Operator
     - Description
     - Example
   * - ``eq``
     - Equality comparison (default)
     - ``version=1.0.0``
   * - ``gt``
     - Greater than
     - ``version__gt=1.0``
   * - ``lt``
     - Less than
     - ``version__lt=2.0``
   * - ``gte``
     - Greater than or equal to
     - ``freq_min_hz__gte=100e6``
   * - ``lte``
     - Less than or equal to
     - ``freq_max_hz__lte=150e6``
   * - ``contains``
     - Match any value containing a sub-string
     - ``catalogue_name__contains=GLEAM``
   * - ``startswith``
     - Match any value starting with a sub-string
     - ``catalogue_name__startswith=GLEAM``
   * - ``endswith``
     - Match any value ending with a sub-string
     - ``catalogue_name__endswith=GLEAM``
   * - ``in``
     - Match any value in a comma-separated list
     - ``catalogue_name__in=GLEAM,LOFAR``

Example:

.. code-block:: text

    GET /catalogue-metadata?version__gt=1.0

This returns catalogues with versions greater than ``1.0``.

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
