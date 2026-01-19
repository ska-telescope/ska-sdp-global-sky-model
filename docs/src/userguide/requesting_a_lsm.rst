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

   a. Querying sources within the specified field of view using spatial indexing (q3c_radial_query)
   b. Retrieving all associated narrowband measurements for matched sources
   c. Retrieving all associated wideband measurements for matched sources
   d. Converting database records to SkySource objects from ska_sdp_datamodels

3. The LSM is written to the shared volume.
4. The metadata is written to.
5. The state is updated to ``COMPLETED``.

If there is a failure the state is updated to ``FAILED`` and a reason is set.

Database Query Details
----------------------

The LSM query uses the following approach:

1. **Spatial Query**: Uses PostgreSQL's q3c extension to efficiently find sources
   within a circular region defined by RA, Dec, and FOV radius (all in radians).

2. **Data Retrieval**: For each matched source, the system retrieves:

   - Source position (RA, Dec) and identifier
   - All wideband measurements from different telescopes
   - All narrowband measurements across different bands and telescopes

3. **Data Model Mapping**: Database records are mapped to the GlobalSkyModel data
   structure defined in ``ska_sdp_datamodels.global_sky_model``:

   - ``GlobalSkyModel``: Top-level container with a dictionary of sources keyed by source ID
   - ``SkySource``: Contains source ID, position, and lists of measurements
   - ``WidebandMeasurement``: Full-band spectral data with polarization info
   - ``NarrowbandMeasurement``: Band-specific measurements with spectral indices

4. **Result Format**: Returns a ``GlobalSkyModel`` object containing a dictionary
   of ``SkySource`` objects (keyed by source ID) with all relevant astronomical
   measurements for sources within the requested field of view.

.. note::
   The ``version`` parameter in query parameters is reserved for future use to
   support multiple GSM catalog versions. Currently, it defaults to "latest"
   but does not affect query results.


