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

    git clone git@gitlab.com:ska-telescope/sdp/ska-sdp-global-sky-model.git --recurse-submodules

Running the application
=======================

The API can be set up to pull data on start up. For ingestion options, see the :doc:`dataset setup guide <../design/sky-model-datasets>`.

In the dataset directory, check a ``catalogue.yaml`` exists for your dataset.
Without this, the API will not render all the columns correctly.
This is also outlined in the :doc:`../design/sky-model-datasets` section.

Start the api with:

.. code-block:: bash

    $ uvicorn ska_sdp_global_sky_model.api.main:app --reload --app-dir ./src

This will make the API available at ``http://127.0.0.1:8000``.

Running the application tests
=============================

The API is tested using the pytest framework alongside FastAPI's TestClient. The tests can be run with:

.. code-block:: bash

    $ make python-test
