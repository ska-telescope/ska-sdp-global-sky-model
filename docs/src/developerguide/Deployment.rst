Deployment Guide
================

Kubernetes Deployment
---------------------

The SKA Global Sky Model is designed to be deployed as a service, accesible to
other deployments via the included API.

Helm Values
~~~~~~~~~~~

The Helm values that are available are listed below:

.. list-table::
    :widths: auto
    :header-rows: 1

    * - Value
      - Default
      - Comment

    * - ``replicaCount``
      - ``1``
      - The amount of replicas to use.

    * - ``env.verbose``
      - ``false``
      - Enable higher verbosity in the logs.

    * - ``env.tmdata_source``
      - ``""``
      - The override for where the GSM data is stored.

    * - ``env.tmdata_keys``
      - ``""``
      - A comma seperated list of keys to download.

    * - ``env.nside``
      - ``128``
      - The value to use for NSIDE.

    * - ``env.wrokers``
      - ``2``
      - The amount of workers to create.

    * - ``container.repository``
      - ``artefact.skao.int/ska-sdp-global-sky-model-api``
      - The repo of the container image to use.

    * - ``container.tag``
      - ``latest``
      - Which version of the container image to use.

    * - ``container.pullPolicy``
      - ``IfNotPresent``
      - What pull policy to use.

    * - ``resources.requests.cpu``
      - ``1000m``
      - The minimum amount of CPU to request.

    * - ``resources.requests.memory``
      - ``512Mi``
      - The minimum amount of memory to request.

    * - ``resources.limits.cpu``
      - ``2000m``
      - The maximum amount of CPU to request.

    * - ``resources.limits.memory``
      - ``2048Mi``
      - The maximum amount of memory to request.

    * - ``ingress.basePath``
      - ``/sdp/global-sky-model``
      - What the base bath should be.

    * - ``ingress.namespaced``
      - ``true``
      - Whether to prepend the base path with the namespace.

    * - ``volume.size``
      - ``10Gi``
      - The size of the disk to use for the GSM data.

    * - ``volume.storageClassName``
      - ``nfss1``
      - What storage class to use for the volume.


Uploading Datasets
~~~~~~~~~~~~~~~~~~

If ``env.tmdata_keys`` was never set, or you would like to add more datasets,
you can copy datasets onto the volume.

First prepare the datasets as per :ref:`Persisting a Dataset` the resulting Tar GZip
file can be uploaded using:

.. code-block:: bash

    $ kubectl exec -n dp-naledi-dominic ska-sdp-global-sky-model-79d5958644-7mg6m -- bash -c "mkdir /datasets/ingests"
    $ kubectl cp -n dp-naledi-dominic gsm_local_data/ASKAP_20250206.tar.gz ska-sdp-global-sky-model-79d5958644-7mg6m:/datasets/ingests/

And then you will need to import that file using:

.. code-block:: bash

    $ kubectl exec -n dp-naledi-dominic ska-sdp-global-sky-model-79d5958644-7mg6m -- \
      bash -c "gsm-download /datasets/ingests/*"


Running the application with Docker
-----------------------------------

Alternatively, the application can be built using the provided Dockerfile.

.. code-block:: bash

    $ make build
    $ make run
