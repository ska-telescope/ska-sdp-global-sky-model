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
                    "ra": 123,
                    "dec": 123,
                    "fov": 123,
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
3. The ``data_model`` needs to be of type ``CsvNamedColumns``.
4. The state needs to exist and have a status of ``INITIALISED``.

Once a flow has been found that matches those criteria, the following is done:

1. The state is updated to ``FLOWING``.
2. The local sky model is retrieved.
3. The LSM is written to the shared volume.
4. The metadata is written to.
5. The state is updated to ``COMPLETED``.

If there is a failure the state is updated to ``FAILED`` and a reason is set.

