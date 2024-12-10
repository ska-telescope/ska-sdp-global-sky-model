Deployment Guide
~~~~~~~~~~~~~~~~

This document complements the guidelines set out in the SKA telescope developer
 portal `<https://developer.skao.int/en/latest/>`_

Kubernetes Deployment
=====================

The SKA Global Sky Model is designed to be deployed as a service, accesible to
other deployments via the included API.

Steps to run the system locally in Minikube
===========================================

The following steps assume that you have cloned the repo, or have a local 
copy of the chart. All given commands assume that you are at the terminal in
your chosen environment.

1. Start Minikube if it is not already running, and optionally enable the 
ingress addon: 

.. code-block:: bash

    $ minikube start
    $ minikube addons enable ingress

2. Change to the chart directory in the repository: ``cd charts/ska-sdp-global-sky-model``.
Make any desired changes to the values files. A local values file can be specified for easy
 management of values used for dev purposes.

3. Optionally, create a new namespace: ``kubectl create namespace [namespace]``.

4. Install the helm chart as follows:

.. code-block:: bash

    $ helm install [deployment-name] charts/ska-sdp-global-sky-model -n [namespace] --values values_local_deployment.yaml

Once the install has completed, you will have the following running:

* The Global Sky Model FastAPI

Running ``minikube service`` will return a URL to connect to the database via the API.

Running the application with Docker Compose
===========================================

Alternatively, the application can be built using the provided docker-compose 
file, with the single command

.. code-block:: bash

    $ docker compose up -d
