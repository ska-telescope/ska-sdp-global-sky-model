Developer Guide
~~~~~~~~~~~~~~~

This document complements the guidelines set out in the `SKA telescope developer portal <https://developer.skao.int/en/latest/>`_


Tooling Pre-requisites
======================

Below are some tools that will be required to work with the SKA Global Sky Model:

- Python 3.10 or later versions: Install page URL: https://www.python.org/downloads/
- Poetry 1.8.2 or later versions: Install page URL: https://python-poetry.org/docs/#installation
- GNU make 4.2 or later versions: Install page URL: https://www.gnu.org/software/make/


Development setup
=================

Clone the repository and its submodules:

.. code-block:: bash

    git clone --recursive git@gitlab.com:ska-telescope/sdp/ska-sdp-global-sky-model.git

Running the application
=======================

The API can be run as a script, provided the connection string to the PostgreSQL database
is updated, using the command:

.. code-block:: bash

    $ uvicorn ska_sdp_global_sky_model.api.app.main:app --reload --host 0.0.0.0 --port 80 --app-dir /usr/src

Running the application tests
=============================

The API is tested using the pytest framework alongside FastAPI's TestClient. The tests can be run with:

.. code-block:: bash

    $ make python-test