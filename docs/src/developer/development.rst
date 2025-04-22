Developer Guide
===============

This document complements the guidelines set
out in the `SKA telescope developer portal <https://developer.skao.int/en/latest/>`_

Tooling Pre-requisites
----------------------

Below are some tools that will be required to work with the SKA Global Sky Model:

- Python 3.10 or later versions: Install page URL: https://www.python.org/downloads/
- Poetry 2.0.1 or later versions: Install page URL: https://python-poetry.org/docs/#installation
- GNU make 4.2 or later versions: Install page URL: https://www.gnu.org/software/make/

Development setup
-----------------

Clone the repository and its submodules:

.. code-block:: bash

    git clone git@gitlab.com:ska-telescope/sdp/ska-sdp-global-sky-model.git --recurse-submodules

Running the application
-----------------------

For instructions on how to run the application locally or via a k9s environment,
see :ref:`deployment`.

Running the application tests
-----------------------------

The API is tested using the pytest framework alongside FastAPI's TestClient.
The tests can be run with:

.. code-block:: bash

    $ make python-test
