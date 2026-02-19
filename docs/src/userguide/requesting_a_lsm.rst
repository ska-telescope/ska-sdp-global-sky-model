Requesting a Local Sky Model
============================

The GSM service watches for requests for LSMs by watching the Configuration DB
for specific Flow entries.

The layout of a flow entry should match the following:

.. code-block:: python

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
        expiry_time=-1,
    )

.. warning::

    Some things to be aware of:

    1. The first item in ``flow.sources`` with ``function="GlobalSkyModel.RequestLocalSkyModel"``
       will be used, any other ones which may have the same function are
       ignored. Note: there should not be more than one source with this
       matching function.
    2. Only 1 query can be done per Flow, so each field must have its own Flow entry.
    3. The output location needs to be specified in ``pvc_subpath``, but the metadata
       file will be put in the first ``<pb_id>/ska-sdm`` parent directory.

Process Flow
------------

The GSM service creates a background thread on startup and monitors all Flow
entries that are of type ``data-product``, for each flow entry the following is
checked:

1. The type needs to be ``data-product``.
2. The function name needs to be ``GlobalSkyModel.RequestLocalSkyModel``.
3. The state needs to exist and have a status of ``INITIALISED``.

Once a flow has been found that matches those criteria, the following is done:

1. The state is updated to ``FLOWING``.
2. The local sky model is retrieved from the database by:

   a. Querying components within the specified field of view using spatial indexing (q3c_radial_query)
   b. Converting database records to SkyComponent objects from ska_sdp_datamodels

3. The LSM is written to the shared volume as a CSV file.
4. The metadata is written to the parent ska-sdm directory.
5. The state is updated to ``COMPLETED``.

If there is a failure the state is updated to ``FAILED`` and a reason is set.

Database Query Details
----------------------

The LSM query uses the following approach:

1. **Spatial Query**: Uses PostgreSQL's q3c extension to efficiently find components
   within a circular region defined by RA, Dec, and FOV radius (all in degrees).

2. **Data Retrieval**: For each matched source, the system retrieves:

   - Component position (RA, Dec) and identifier
   - Stokes parameters (I, Q, U, V polarization)
   - Source shape parameters (major/minor axes, position angle)
   - Spectral index information

3. **Data Model Mapping**: Database records are mapped to the GlobalSkyModel data
   structure defined in ``ska_sdp_datamodels.global_sky_model``:

   - ``GlobalSkyModel``: Top-level container with metadata and a dictionary of components
   - ``SkyComponent``: Contains component ID, position (RA, Dec), Stokes parameters,
     morphology (major_ax, minor_ax, pos_ang), and spectral index as a list of coefficients

4. **Result Format**: Returns a ``GlobalSkyModel`` object containing a dictionary
   of ``SkyComponent`` objects (keyed by component ID) with all relevant astronomical
   measurements for components within the requested field of view.

5. **Output Format**: The LSM is written as a CSV file with named columns matching the
   ``SkyComponent`` dataclass fields. Metadata is written as a YAML file in the parent
   ``ska-sdm`` directory as specified in the Flow configuration.

.. note::
   The ``version`` parameter allows you to specify which catalogue version to query.
   This supports multiple GSM catalogue versions in the database. Use semantic versioning
   (e.g., \"1.0.0\", \"0.1.0\") or \"latest\" to query the most recent version.
