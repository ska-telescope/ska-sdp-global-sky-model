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
                    "ra": 2.9670,
                    "dec": -0.1745,
                    "fov": 0.0873,
                    "version": "latest",
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
    3. The output location needs to be specified in ``pvc_subpath``, but the metadata
       file will be put in the first ``<pb_id>/ska-sdm`` parent directory.

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

   a. Querying components within the specified field of view using spatial indexing (q3c_radial_query)
   b. Converting database records to SkyComponent objects from ska_sdp_datamodels

3. The LSM is written to the shared volume as a CSV file.
4. The metadata is written to the parent directory.
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

+----+----------------+---------+----------------+-----------+-----------+-------+----------+----------+---------+----------+--------------+-------+-------+-------+
| Id | Healpix_index  | Version | Component_id   | Ra        | Dec       | I_pol | Major_ax | Minor_ax | Pos_ang | Spec_idx | Log_spec_idx | Q_pol | U_pol | V_pol |
+====+================+=========+================+===========+===========+=======+==========+==========+=========+==========+==============+=======+=======+=======+
| 1  | 72434864       | 0.0.0   | J023255-053134 | 38.230309 | -5.526247 | None  | None     | None     | None    | None     | None         | None  | None  | None  |
+----+----------------+---------+----------------+-----------+-----------+-------+----------+----------+---------+----------+--------------+-------+-------+-------+

Local Sky Model
...............

To access the LSM (filtered list of components) navigate to
``GET /local_sky_model?ra={ra}&dec={dec}&fov={fov}&version={version}``

where:

.. list-table::
   :widths: 20 50 20 10
   :header-rows: 1

   * - Parameter
     - Description
     - Data Type
     - Required
   * - ``ra``
     - The right ascension of the centre of the cone search (in degrees)
     - float
     - Yes
   * - ``dec``
     - The declination of the centre of the cone search (in degrees)
     - float
     - Yes
   * - ``fov``
     - The field of view of the cone search (in arcminutes)
     - float
     - Yes
   * - ``version``
     - The version string of the GSM to select from (not implemented)
     - string
     - Yes
