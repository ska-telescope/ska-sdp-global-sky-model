.. _get_data:

Downloading and uploading catalogue data
========================================

.. _download_data:

Accessing data
--------------

You can use the :ref:`cli_use` for this, or, see below for other options.

Helm Setup
~~~~~~~~~~

To download the datasets via helm, set the following ``values.yaml`` variables:

.. code-block:: yaml

    env:
      tmdata_source: "car:sdp/ska-sdp-global-sky-model?0.2.0"
      tmdata_keys: "ska/sdp/gsm/ASKAP_20250206.tar.gz,ska/sdp/gsm/Murchison_Widefield_Array_20250218.tar.gz"

Local Poetry Setup (On Startup)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Or, if you are running locally and not using helm, you can use the local dataset files. Set the following ENV variables:

.. code-block:: bash

    $ export TMDATA_SOURCE=file://$(pwd)/tmdata
    $ export TMDATA_KEYS=ska/sdp/gsm/ASKAP_20250206.tar.gz,ska/sdp/gsm/Murchison_Widefield_Array_20250218.tar.gz

When the service starts, these datasets will load from the ``tmdata/`` directory.

Manual Setup
~~~~~~~~~~~~

Finally, you have the option to manually fetch the data by running this helper:

.. code-block:: bash

    $ make manual-download


Fetching outside of the GSM
~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you want just the datafiles, you can use the ``ska-telmodel`` command:

.. code-block:: bash

    $ ska-telmodel --sources=car:sdp/ska-sdp-global-sky-model?0.2.0 cp ska/sdp/gsm/ASKAP_20250206.tar.gz
    $ ska-telmodel --sources=car:sdp/ska-sdp-global-sky-model?0.2.0 cp ska/sdp/gsm/Murchison_Widefield_Array_20250218.tar.gz

Persisting a Dataset
--------------------

If you want to persist a dataset for an instance that is not running yet:

  - Finish ingestion using :ref:`gms_ingest`
  - Compress the directory using the following command:

.. code-block:: bash

    $ cd ${GSM_DATA}
    $ tar cf - "<directory>" | pigz -9 > "<directory (without spaces)>_$(date "+%Y%m%d").tar.gz"

Uploading Datasets
~~~~~~~~~~~~~~~~~~

If ``env.tmdata_keys`` was never set, or you would like to add more datasets,
you can copy datasets onto the persistent volume that the GSM connects to when deployed in k8s.

First prepare the datasets as per above ("Persisting a Dataset"). The resulting Tar GZip
file can be uploaded with the following command.
Remember to replace ``<namespace>`` with the namespace where the GSM is running and
``<gsm-pod>`` with the actual pod name hosting the GSM.

.. code-block:: bash

    $ kubectl exec -n <namespace> <gsm-pod> -- bash -c "mkdir /datasets/ingests"
    $ kubectl cp -n <namespace> gsm_local_data/ASKAP_20250206.tar.gz <gsm-pod>:/datasets/ingests/

And then you will need to import that file using the CLI:

.. code-block:: bash

    $ kubectl exec -n <namespace> <gsm-pod> -- \
      bash -c "gsm-download /datasets/ingests/*"
