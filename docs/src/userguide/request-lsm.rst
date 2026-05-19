.. _request_lsm_processing:

Requesting Local Sky Model data
-------------------------------

SDP processing pipelines
^^^^^^^^^^^^^^^^^^^^^^^^

The processing script requesting an LSM on behalf of a data processing
pipeline has to do it by generating data flow entries in the
SDP Configuration Database (see :ref:`design page <sdp_request>`).

The flow needs to be of kind ``data-product`` and its
layout should match the following:

.. code-block:: python

    from ska_sdp_config.entity.flow import DataProduct, Flow, FlowSource, PVCPath

    Flow(
        key=Flow.Key(
            pb_id=pb_id,
            name=f"local-sky-model",
        ),
        sink=DataProduct(
            data_dir=PVCPath(
                k8s_namespaces=[],
                k8s_pvc_name="",
                pvc_mount_path="/mnt/data",
                pvc_subpath=pathlib.Path(f"product/{eb_id}/ska-sdp/{pb_id}/ska-sdm"),
            ),
            paths=[],
        ),
        sources=[
            FlowSource(
                uri="gsm://request/lsm",
                function="GlobalSkyModel.RequestLocalSkyModel",
                parameters={
                    "ra_deg": 2.9670,
                    "dec_deg": -0.1745,
                    "fov_deg": 0.0873,
                    "version": "latest",
                    "catalogue_name": "name",
                    "sub_path": "sky/{field1}/sky_model.csv",
                },
            )
        ],
        data_model="CsvNamedColumns",
        expiry_time=1600,
    )

Some things to be aware of:

    1. Each source with ``function="GlobalSkyModel.RequestLocalSkyModel"``
       will be used. Each source will generate its own local sky model, and
       therefore the ``sub_path`` should be unique, else it will be overwritten.
    2. The output location needs to be specified by the ``sub_path`` parameter, relative to
       the ``pvc_subpath``, and the metadata file will be in the directory specified by ``pvc_subpath``.
    3. ``version`` is not required to be set, but will default to ``version=latest``
       when it is not set.
    4. ``catalogue_name`` is required when submitting a query via processing scripts.

Processing data flow requests
.............................

The GSM service creates a background thread on startup and monitors all Flow
entries that are of type ``data-product``, for each flow entry the following is
checked:

1. The type needs to be ``data-product``.
2. The function name needs to be ``GlobalSkyModel.RequestLocalSkyModel``.
3. The state needs to exist and have a status of ``INITIALISED``.

Once a flow has been found that matches those criteria, the following is done:

1. The state is updated to ``FLOWING``.
2. The local sky model is retrieved.

   a. Querying components within the specified field of view using spatial indexing
   b. Converting database records to SkyComponent objects from ska_sdp_datamodels

3. The LSM is written to the shared volume as a CSV file.
4. The metadata in YAML format is written to the parent directory.
5. The state is updated to ``COMPLETED``.

If there is a failure the state is updated to ``FAILED`` and a reason is set.

Output CSV file
...............

The output CSV file is described at :ref:`lsm_file`.


.. _lsm_browser:

Viewing LSM data in a browser
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For users who would like to inspect the data visually, a view has been provided.
The table view is available through a browser interface.
It is at the ``/local-sky-model`` endpoint.

Local Sky Model
...............

To access the LSM (filtered list of components) navigate to
``GET /local-sky-model?ra_deg={ra_deg}&dec_deg={dec_deg}&fov_deg={fov_deg}&version={version}&catalogue_name={catalogue_name}``

where:

.. list-table::
   :widths: 20 50 20 10
   :header-rows: 1

   * - Parameter
     - Description
     - Data Type
     - Required
   * - ``ra_deg``
     - The right ascension of the centre of the cone search (in degrees).
     - float
     - Yes
   * - ``dec_deg``
     - The declination of the centre of the cone search (in degrees).
     - float
     - Yes
   * - ``fov_deg``
     - The field of view of the cone search (in degrees).
     - float
     - Yes
   * - ``version``
     - The version string of the GSM to select from (only supports semantic versioning).
     - string
     - No
   * - ``catalogue_name``
     - The catalogue name string of the GSM to select from.
     - string
     - No
   * - ``page``
     - The page number currently being viewed.
     - int
     - No
   * - ``page_size``
     - The number of components per page.
     - int
     - No

This can be downloaded as a CSV by adding the parameter ``format=csv``.
Note that if the returned data match multiple catalogues/catalogue versions,
then the downloaded CSV file will have all the data appended, with header
items at the start of each catalogue data (i.e. header items will appear in between data items).

To be able to view all components one can use:

.. code-block:: text

    GET /local-sky-model?ra_deg=0&dec_deg=0&fov_deg=180

Alternatively, the ``/components`` endpoint can also be used:

.. code-block:: text

    GET /components

Note that this end point does not allow downloading the data.

Filtering examples
^^^^^^^^^^^^^^^^^^

You can filter results by specifying extra query parameters using the ``column__operator=value`` syntax.
The keys ``i_pol_jy__gte`` and ``i_pol_jy__lte`` are passed exactly as written, whether in a query string or
in a parameters dictionary.

To specify a range filter in data flow parameters, include the keys directly:

.. code-block:: python

  parameters = {
    "ra_deg": 70,
    "dec_deg": 4,
    "fov_deg": 1,
    "catalogue_name": "example",
    "version": "1.0.0",
    "i_pol_jy__gte": 0.5,
    "i_pol_jy__lte": 1.0,
  }

This restricts the result to components whose ``i_pol_jy`` value is between 0.5 Jy and 1.0 Jy inclusive.

For an equality filter:

.. code-block:: python

  parameters = {
    ...,
    "catalogue_name": "example",
  }

This will select only rows where ``catalogue_name`` matches ``example`` exactly.

Some other example filter parameters based on catalogue metadata:

* ``freq_min_hz__gt=150e6``

  * Return sky components from a GSM catalogue with a minimum frequency
    greater than 150 MHz.

* ``author__in="Alice,Bob"``

  * Return sky components from a GSM catalogue with an author matching
    either Alice or Bob.

* ``author__startswith="SDP"``

  * Return sky components from a GSM catalogue where the author name
    starts with "SDP".

* ``catalogue_name__contains="GLEAM"``

  * Return sky components from a GSM catalogue where the catalogue name
    contains "GLEAM".

To achieve the same range filter in an HTTP request, add the relevant parameters to the query string:

.. code-block:: text

  GET /local-sky-model?ra_deg=70&dec_deg=4&fov_deg=1&catalogue_name=example&version=1.0.0&i_pol_jy__gte=0.5&i_pol_jy__lte=1.0

For an equality filter, simply use ``column=value``:

.. code-block:: text

  GET /local-sky-model?catalogue_name=example

For a full list of supported operators and more examples, see:

.. toctree::
  :maxdepth: 1

  querying-data
