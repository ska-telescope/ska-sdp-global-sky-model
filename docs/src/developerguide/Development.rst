Developer Guide
~~~~~~~~~~~~~~~

This document complements the guidelines set out in the `SKA telescope developer portal <https://developer.skao.int/en/latest/>`_


Tooling Pre-requisites
======================

Below are some tools that will be required to work with the SKA Global Sky Model:

- Python 3.10 or later versions: Install page URL: https://www.python.org/downloads/
- Poetry 2.0.1 or later versions: Install page URL: https://python-poetry.org/docs/#installation
- GNU make 4.2 or later versions: Install page URL: https://www.gnu.org/software/make/


Development setup
=================

Clone the repository and its submodules:

.. code-block:: bash

    git clone --recursive git@gitlab.com:ska-telescope/sdp/ska-sdp-global-sky-model.git

Running the application
=======================

A pre-requisite to running the API is to ingest data to `datasets/`. The options for which data to use are outlined in the :doc:`dataset setup guide <../userguide/sky_model_datasets>`. For a quick start, run the ingest script:

.. code-block:: bash

    $ python cli/ingest_sm.py gleam

Optionally (but recommended), create a ``catalogue.yaml`` for your dataset, this is also outlined in the :doc:`../userguide/sky_model_datasets` section.

Start the api with:

.. code-block:: bash

    $ uvicorn ska_sdp_global_sky_model.api.main:app --reload --app-dir ./src

This will make the API available at `http://127.0.0.1:8000`. 

Running the application tests
=============================

The API is tested using the pytest framework alongside FastAPI's TestClient. The tests can be run with:

.. code-block:: bash

    $ make python-test