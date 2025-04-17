.. _cli_use:

GSM CLI use
===========

This section describes the two CLI commands the GSM, ``gsm-download`` and ``gsm-ingest``.

Overview
--------

``gsm-download``: For fetching and extracting catalogue .tar files
``gsm-ingest``: For converting catalogues into a format the GSM can use.

gsm-download
------------

This command fetches and extracts ``.tar.gz`` files into ``DATASET_ROOT``.

.. code-block:: bash

    gsm-download <dataset 1> [<dataset 2> ...]

For each dataset:
  - If it exists locally, it copies it into ``DATASET_ROOT`` and extracts the tar file.
  - If it doesn't exist, it looks up the download URL from ``TMDATA_SOURCE`` and then extracts the tar file.

For example, if the API is already running, run in the same context as the API:

.. code-block:: bash

    TMDATA_SOURCE=car:sdp/ska-sdp-global-sky-model?0.2.0 gsm-download --verbose ska/sdp/gsm/ASKAP_20250206.tar.gz ska/sdp/gsm/Murchison_Widefield_Array_20250218.tar.gz

See below for further examples.

Local File
**********

For a local file, copy the file into a location on the POD. Then run the following command:

.. code-block:: bash

    $ gsm-download <file>.tar.gz


TMData File
***********

For a TMData referenced file, you only need to give the key.

.. code-block:: bash

    $ gsm-download <key to file>.tar.gz

If the file is not in a default or setup source, you can specify a different source:

.. code-block:: bash

    $ TMDATA_SOURCE="<TM Data source>" gsm-download <key to file>.tar.gz


gsm-ingest
----------

This command processes data from a catalogue and stores it in ``DATASET_ROOT``.

.. code-block:: bash

   gsm-ingest <catalogue 1> [<catalogue 2> ...]

Each catalogue arg can be gleam, racs or any local file path that points to your data. See below for examples of each.

Note that this ingest command can do multiple imports at the same time. By just
adding more names/files to the command. For example: ``gsm-ingest gleam racs <csv file>``.
And each ingest will run one after the other.

The ingest process does not create the ``catalogue.yaml`` which is required for
retrieving more details from the API.


GLEAM
~~~~~

This dataset is from "Murchison Widefield Array" and is downloaded while ingesting.

.. code-block:: bash

    $ DATASET_ROOT=<directory to use> gsm-ingest gleam

RACS
~~~~

This dataset is from "ASKAP", this catalogue does require 2 files to present within
the dataset directory before ingesting.

We require 2 CSVs in the ``DATASET_ROOT/ingest`` directory:

* ``AS110_Derived_Catalogue_racs_mid_components_v01_15373.csv``
* ``AS110_Derived_Catalogue_racs_dr1_gaussians_galacticcut_v2021_08_v02_5723.csv``

.. code-block:: bash

    $ DATASET_ROOT=<directory to use> gsm-ingest racs

RCAL
~~~~

This dataset is data used for the Realtime Calibration. And requires you to
include your own CSV files.


.. code-block:: bash

    $ DATASET_ROOT=<directory to use> gsm-ingest <csv file(s)>


Environment Variables
---------------------

On startup there are 2 environment variables that are used to determine which
datasets to download and prepare for use.

* ``TMDATA_SOURCE`` : is an optional environment variable that can be set to a path
  that can be used for the telescope model source data. If blank the system
  will look in the default list of sources.
* ``TMDATA_KEYS`` : is an optional environment variable which should contain a comma seperated list
  of keys that should be downloaded on startup. The GSM system assumes that these files are considered
  as large files, and as such will download the listed file. These files should be ``.tar.gz`` compressed
  files created in the `Downloading an Existing Dataset`_ section


