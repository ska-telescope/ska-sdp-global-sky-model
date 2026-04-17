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
                pvc_subpath=pathlib.Path(f"product/{eb_id}/ska-sdp/{pb_id}/ska-sdm/sky/{field_id}"),
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

    1. The first item in ``flow.sources`` with ``function="GlobalSkyModel.RequestLocalSkyModel"``
       will be used, any other ones which may have the same function are
       ignored. Note: there should not be more than one source with this
       matching function.
    2. Only 1 query can be done per Flow, so each field must have its own Flow entry.
    3. The output location needs to be specified by the ``sub_path`` parameter, relative to 
       the ``pvc_subpath``, and the metadata file will be in the directory specified by ``pvc_subpath``.

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

For users who would like to inspect the data visually, two views have been provided.
The two table views are available through a browser interface.
These are at the `/components` and the `/local_sky_model` endpoints.

Components
..........

The components endpoint gives a table of the components that have been added to the GSM.
To access these on a local instance of the GSM, navigate to ``GET /components``

The following table will be displayed:

+----+----------------+---------+----------------+----------------+-----------+-----------+----------+----------+----------+---------+----------+--------------+-------------+
| Id | Healpix_index  | Version | Catalogue_name | Component_id   | Ra_deg    | Dec_deg   | I_pol_jy | A_arcsec | B_arcsec | Pa_deg  | Spec_idx | Log_spec_idx | Ref_freq_hz |
+====+================+=========+================+================+===========+===========+==========+==========+==========+=========+==========+==============+=============+
| 1  | 72434864       | 0.0.0   | generic        | J023255-053134 | 38.230309 | -5.526247 | None     | None     | None     | None    | None     | None         | 17000000    |
+----+----------------+---------+----------------+----------------+-----------+-----------+----------+----------+----------+---------+----------+--------------+-------------+

Local Sky Model
...............

To access the LSM (filtered list of components) navigate to
``GET /local_sky_model?ra_deg={ra_deg}&dec_deg={dec_deg}&fov_deg={fov_deg}&version={version}&catalogue_name={catalogue_name}``

where:

.. list-table::
   :widths: 20 50 20 10
   :header-rows: 1

   * - Parameter
     - Description
     - Data Type
     - Required
   * - ``ra_deg``
     - The right ascension of the centre of the cone search (in degrees)
     - float
     - Yes
   * - ``dec_deg``
     - The declination of the centre of the cone search (in degrees)
     - float
     - Yes
   * - ``fov_deg``
     - The field of view of the cone search (in degrees)
     - float
     - Yes
   * - ``version``
     - The version string of the GSM to select from (only supports semantic versioning)
     - string
     - No
   * - ``catalogue_name``
     - The catalogue name string of the GSM to select from
     - string
     - Yes


Filtering examples
------------------

You can filter results by specifying extra query parameters using the ``column__operator=value`` syntax.
The keys ``i_pol_jy__gte`` and ``i_pol_jy__lte`` are passed exactly as written, whether in a query string or
in a parameters dictionary.

To specify a range filter in an HTTP request, add the relevant parameters to the query string:

.. code-block:: text

  GET /local-sky-model?ra_deg=70&dec_deg=4&fov_deg=1&catalogue_name=example&version=1.0.0&i_pol_jy__gte=0.5&i_pol_jy__lte=1.0

This restricts the result to components whose ``i_pol_jy`` value is between 0.5 Jy and 1.0 Jy inclusive.

For an equality filter, simply use ``column=value``:

.. code-block:: text

  GET /local-sky-model?catalogue_name=example

This will select only rows where ``catalogue_name`` matches ``example`` exactly.

To achieve the same range filter in data flow parameters, include the keys directly:

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

For an equality filter:

.. code-block:: python

  parameters = {
    ...,
    "catalogue_name": "example",
  }

See :ref:`querying_data` for a full list of supported operators and more examples.
