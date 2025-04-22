.. _deployment:

Deployment Guide
================

Kubernetes Deployment
---------------------

The SKA Global Sky Model is designed to be deployed as a service, accessible to
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
      - The number of replicas to use.

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

    * - ``env.workers``
      - ``1``
      - The number of workers to create.

    * - ``container.repository``
      - ``artefact.skao.int/ska-sdp-global-sky-model-api``
      - The repo of the container image to use.

    * - ``container.tag``
      - ``0.2.0``
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
      - What the base path should be.

    * - ``ingress.namespaced``
      - ``true``
      - Whether to prepend the base path with the namespace.

    * - ``volume.size``
      - ``10Gi``
      - The size of the disk to use for the GSM data.

    * - ``volume.storageClassName``
      - ``nfss1``
      - What storage class to use for the volume.

Run the following command to install the chart in a kubernetes cluster
(this command works if you have the repository cloned locally).
Note that some variables will need replacing first, which are described below.

.. code-block:: bash

    $ helm install sdp-gsm charts/ska-sdp-global-sky-model -n <namespace> \
        --values <my-values.yaml>

``<namespace>`` is the namespace where you want to install the GSM, and
``<my-values.yaml>`` is the custom values file you may create. This is
optional and the ``--values`` option can be omitted.


Running the application locally
-------------------------------

Alternatively, the application can be built and run locally by
using the provided Dockerfile, via the following make targets:

.. code-block:: bash

    $ make build
    $ make run

Without Docker, you can use the following make target to run the
site on localhost. This requires that you have downloaded the
catalogue data already via one of the options described in
:ref:`download_data`.

.. code-block:: bash

    $ make run-local

or

.. code-block:: bash

    $ uvicorn ska_sdp_global_sky_model.api.main:app --reload --app-dir ./src

This will make the API available at ``http://127.0.0.1:8000``.