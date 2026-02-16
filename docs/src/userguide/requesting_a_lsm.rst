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
       ignored. Note: there should not be more than one component with this
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

1. **Spatial Query**: Uses PostgreSQL's q3c extension to efficiently find sources
   within a circular region defined by RA, Dec, and FOV radius (all in radians).

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
   The ``version`` parameter in query parameters is reserved for future use to
   support multiple GSM catalog versions. Currently, it defaults to "latest"
   but does not affect query results.


Examples
--------

The following examples demonstrate how to request a Local Sky Model using the
Configuration DB Flow interface. These examples use test catalog coordinates
that can be populated using the test data included in the repository.

Example 1: Basic LSM Request
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This example shows how to request a Local Sky Model for a field centered at
RA=45° (0.7854 radians), Dec=2° (0.0349 radians) with a 2-degree radius
field of view (0.0349 radians).

.. code-block:: python

    import pathlib
    import ska_sdp_config
    from ska_sdp_config.entity import Flow
    from ska_sdp_config.entity.flow import DataProduct, FlowSource, PVCPath

    # Connection parameters
    eb_id = "eb-demo-20260216-12345"
    pb_id = "pb-demo-20260216-12345"
    field_id = "field_test"

    # Query parameters (radians)
    ra = 0.7854   # 45 degrees
    dec = 0.0349  # 2 degrees
    fov = 0.0349  # 2 degree radius

    # Create Configuration DB connection
    config = ska_sdp_config.Config()

    # Create a transaction and add the Flow
    for txn in config.txn():
        flow = Flow(
            key=Flow.Key(
                pb_id=pb_id,
                name=f"local-sky-model-{field_id}",
            ),
            sink=DataProduct(
                data_dir=PVCPath(
                    k8s_namespaces=[],
                    k8s_pvc_name="",
                    pvc_mount_path="/mnt/data",
                    pvc_subpath=pathlib.Path(
                        f"product/{eb_id}/ska-sdp/{pb_id}/ska-sdm/sky/{field_id}"
                    ),
                ),
                paths=[],
            ),
            sources=[
                FlowSource(
                    uri="gsm://request/lsm",
                    function="GlobalSkyModel.RequestLocalSkyModel",
                    parameters={
                        "ra": ra,
                        "dec": dec,
                        "fov": fov,
                        "version": "latest",
                    },
                )
            ],
            data_model="CsvNamedColumns",
            expiry_time=-1,
        )
        txn.flow.add(flow)
        
        # Initialize state
        txn.flow.state(flow).create({"status": "INITIALISED"})

    # The GSM watcher will process this Flow and create:
    # - CSV file: /mnt/data/product/{eb_id}/ska-sdp/{pb_id}/ska-sdm/sky/{field_id}/local_sky_model.csv
    # - Metadata: /mnt/data/product/{eb_id}/ska-sdp/{pb_id}/ska-sdm/ska-data-product.yaml

    config.close()


Example 2: Query Based on Test Catalog
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Using the test catalog data in ``tests/data/test_catalog_1.csv``, which contains
components around RA~44-46° and Dec~0-4°, this example demonstrates querying a
subset of those sources:

.. code-block:: python

    import pathlib
    import math
    import ska_sdp_config
    from ska_sdp_config.entity import Flow
    from ska_sdp_config.entity.flow import DataProduct, FlowSource, PVCPath

    def deg_to_rad(degrees):
        """Convert degrees to radians"""
        return degrees * math.pi / 180.0

    # Define field center and radius
    ra_deg = 45.0    # Center RA in degrees
    dec_deg = 2.0    # Center Dec in degrees
    fov_deg = 1.5    # Field of view radius in degrees

    # Convert to radians
    ra = deg_to_rad(ra_deg)
    dec = deg_to_rad(dec_deg)
    fov = deg_to_rad(fov_deg)

    # Execution and processing block IDs
    eb_id = "eb-test-20260216-00001"
    pb_id = "pb-test-20260216-00001"
    field_id = "test_field_01"

    config = ska_sdp_config.Config()

    for txn in config.txn():
        flow = Flow(
            key=Flow.Key(pb_id=pb_id, name=f"local-sky-model-{field_id}"),
            sink=DataProduct(
                data_dir=PVCPath(
                    k8s_namespaces=[],
                    k8s_pvc_name="",
                    pvc_mount_path="/mnt/data",
                    pvc_subpath=pathlib.Path(
                        f"product/{eb_id}/ska-sdp/{pb_id}/ska-sdm/sky/{field_id}"
                    ),
                ),
                paths=[],
            ),
            sources=[
                FlowSource(
                    uri="gsm://request/lsm",
                    function="GlobalSkyModel.RequestLocalSkyModel",
                    parameters={"ra": ra, "dec": dec, "fov": fov, "version": "latest"},
                )
            ],
            data_model="CsvNamedColumns",
            expiry_time=-1,
        )
        txn.flow.add(flow)
        txn.flow.state(flow).create({"status": "INITIALISED"})

    config.close()

    print(f"LSM request created for field centered at ({ra_deg}°, {dec_deg}°)")
    print(f"Field of view radius: {fov_deg}°")
    print(f"Output: /mnt/data/product/{eb_id}/ska-sdp/{pb_id}/ska-sdm/sky/{field_id}/")


Example 3: Multiple Fields
~~~~~~~~~~~~~~~~~~~~~~~~~~~

To request LSMs for multiple fields (e.g., mosaic observations), create separate
Flow entries for each field:

.. code-block:: python

    import pathlib
    import math
    import ska_sdp_config
    from ska_sdp_config.entity import Flow
    from ska_sdp_config.entity.flow import DataProduct, FlowSource, PVCPath

    def deg_to_rad(deg):
        return deg * math.pi / 180.0

    # Define multiple fields
    fields = [
        {"id": "field_01", "ra_deg": 44.5, "dec_deg": 1.5, "fov_deg": 1.0},
        {"id": "field_02", "ra_deg": 45.5, "dec_deg": 2.5, "fov_deg": 1.0},
        {"id": "field_03", "ra_deg": 44.0, "dec_deg": 3.0, "fov_deg": 1.0},
    ]

    eb_id = "eb-mosaic-20260216-00001"
    pb_id = "pb-mosaic-20260216-00001"

    config = ska_sdp_config.Config()

    for field in fields:
        for txn in config.txn():
            flow = Flow(
                key=Flow.Key(pb_id=pb_id, name=f"local-sky-model-{field['id']}"),
                sink=DataProduct(
                    data_dir=PVCPath(
                        k8s_namespaces=[],
                        k8s_pvc_name="",
                        pvc_mount_path="/mnt/data",
                        pvc_subpath=pathlib.Path(
                            f"product/{eb_id}/ska-sdp/{pb_id}/ska-sdm/sky/{field['id']}"
                        ),
                    ),
                    paths=[],
                ),
                sources=[
                    FlowSource(
                        uri="gsm://request/lsm",
                        function="GlobalSkyModel.RequestLocalSkyModel",
                        parameters={
                            "ra": deg_to_rad(field["ra_deg"]),
                            "dec": deg_to_rad(field["dec_deg"]),
                            "fov": deg_to_rad(field["fov_deg"]),
                            "version": "latest",
                        },
                    )
                ],
                data_model="CsvNamedColumns",
                expiry_time=-1,
            )
            txn.flow.add(flow)
            txn.flow.state(flow).create({"status": "INITIALISED"})
        
        print(f"Created LSM request for {field['id']}")

    config.close()


Monitoring Request Status
~~~~~~~~~~~~~~~~~~~~~~~~~~

To check the status of your LSM request:

.. code-block:: python

    import ska_sdp_config
    from ska_sdp_config.entity import Flow

    config = ska_sdp_config.Config()
    pb_id = "pb-demo-20260216-12345"
    flow_name = "local-sky-model-field_test"

    for txn in config.txn():
        flow_key = Flow.Key(pb_id=pb_id, name=flow_name)
        state = txn.flow.state(flow_key).get()
        
        if state:
            print(f"Status: {state.get('status')}")
            if 'reason' in state:
                print(f"Reason: {state['reason']}")
            if 'last_updated' in state:
                import datetime
                timestamp = datetime.datetime.fromtimestamp(state['last_updated'])
                print(f"Last updated: {timestamp}")
        else:
            print("Flow not found or has no state")

    config.close()

Expected states:
- ``INITIALISED``: Request created, waiting for processing
- ``FLOWING``: Currently being processed
- ``COMPLETED``: Successfully completed, LSM files are ready
- ``FAILED``: Processing failed, check the reason field


Output Files
~~~~~~~~~~~~

Once processing completes successfully, you will find:

1. **CSV File**: Contains the sky model data with columns matching the ``SkyComponent``
   dataclass fields:
   
   - ``component_id``: Source identifier
   - ``ra``, ``dec``: Position in radians
   - ``i_pol``, ``q_pol``, ``u_pol``, ``v_pol``: Stokes parameters
   - ``major_ax``, ``minor_ax``, ``pos_ang``: Source shape (arcseconds and degrees)
   - ``spec_idx``: Spectral index coefficients (list/array)
   - ``log_spec_idx``: Boolean indicating if spectral index is logarithmic

2. **Metadata YAML**: Located in the ``ska-sdm`` parent directory, contains:
   
   - Execution block ID
   - File path reference to the CSV file
   - Column names and header information
   - Number of components


Example CSV Output Format:

.. code-block:: text

    # (component_id,ra,dec,i_pol,major_ax,minor_ax,pos_ang,spec_idx,log_spec_idx,...) = format
    # NUMBER_OF_COMPONENTS: 42
    J025837+035057,0.779234,0.067145,0.835419,142.417,132.7302,3.451346,"[-0.419238]",false
    J030420+022029,0.804234,0.040834,0.29086,137.107,134.2583,-0.666618,"[-1.074094]",false
    ...
