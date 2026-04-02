.. _querying_data:

Querying Data
-------------

The GSM query endpoints use a shared filtering syntax based on query parameters.

This applies to:

- ``/catalogue-metadata`` for catalogue metadata rows.
- ``/local-sky-model`` for sky component rows (after cone search and catalogue selection).

Filter syntax
^^^^^^^^^^^^^

Use ``column__operator=value``. If no operator is provided, equality is used.

Supported operators
^^^^^^^^^^^^^^^^^^^

.. list-table:: Supported filter operators
   :widths: 20 50 30
   :header-rows: 1

   * - Operator
     - Description
     - Example
   * - ``eq``
     - Equality comparison (default)
     - ``version=1.0.0``
   * - ``ne``
     - Not equal
     - ``catalogue_name__ne=GLEAM``
   * - ``gt``
     - Greater than
     - ``version__gt=1.0``
   * - ``gte``
     - Greater than or equal to
     - ``version__gte=2.0``
   * - ``lt``
     - Less than
     - ``version__lt=2.0``
   * - ``lte``
     - Less than or equal to
     - ``version__lte=3.0``
   * - ``contains``
     - Case-insensitive substring search
     - ``catalogue_name__contains=GLEAM``
   * - ``startswith``
     - Case-insensitive prefix search
     - ``catalogue_name__startswith=GL``
   * - ``endswith``
     - Case-insensitive suffix search
     - ``catalogue_name__endswith=FAR``
   * - ``in``
     - Match any value in a comma-separated list
     - ``catalogue_name__in=GLEAM,LOFAR``

Range queries
^^^^^^^^^^^^^

Combine minimum and maximum bounds using ``gte`` and ``lte``.

Metadata example:

.. code-block:: text

    GET /catalogue-metadata?version__gte=2.0&version__lte=3.0

Flux example:

.. code-block:: text

    GET /local-sky-model?ra_deg=70&dec_deg=4&fov_deg=1&catalogue_name=example&version=1.0.0&i_pol_jy__gte=0.5&i_pol_jy__lte=1.0

Sorting, field selection, and limits
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The shared query parser also supports:

- ``sort`` for ordering (prefix with ``-`` for descending).
- ``fields`` for selecting columns.
- ``limit`` for maximum rows returned.
