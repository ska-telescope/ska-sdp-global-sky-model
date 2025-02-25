Sky Model Dataset
=================

This section will describe how to create new datasets, and upload them into
the GSM Service.

Existing Datasets
-----------------

There are 2 datasets that are available for use. This section will describe
how to get them in the various setups.

Helm Install
~~~~~~~~~~~~

Set the following ``values`` variables:

.. code-block:: yaml

    env:
      tmdata_source: "car:sdp/ska-sdp-global-sky-model?0.2.0"
      tmdata_keys: "ska/sdp/gsm/ASKAP_20250206.tar.gz,ska/sdp/gsm/Murchison_Widefield_Array_20250218.tar.gz"

Local Poetry Setup (On Startup)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Set the following ENV variables:

.. code-block:: bash

    $ export TMDATA_SOURCE=car:sdp/ska-sdp-global-sky-model?0.2.0
    $ export TMDATA_KEYS=ska/sdp/gsm/ASKAP_20250206.tar.gz,ska/sdp/gsm/Murchison_Widefield_Array_20250218.tar.gz

Local Poetry Setup
~~~~~~~~~~~~~~~~~~

You can run this helper to fetch the datasets:

.. code-block:: bash

    $ make manual-download

Existing running install
~~~~~~~~~~~~~~~~~~~~~~~~

If the api is already running, run in the same context as the API:

.. code-block:: bash

    TMDATA_SOURCE=car:sdp/ska-sdp-global-sky-model?0.2.0 gsm-download --verbose ska/sdp/gsm/ASKAP_20250206.tar.gz ska/sdp/gsm/Murchison_Widefield_Array_20250218.tar.gz

Fetching outside of the GSM
~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you want just the datafiles, you can use the ``ska-telmodel`` command:

.. code-block:: bash

    $ ska-telmodel --sources=car:sdp/ska-sdp-global-sky-model?0.2.0 cp ska/sdp/gsm/ASKAP_20250206.tar.gz
    $ ska-telmodel --sources=car:sdp/ska-sdp-global-sky-model?0.2.0 cp ska/sdp/gsm/Murchison_Widefield_Array_20250218.tar.gz

Creating a Dataset
------------------

This section will describe how to prepare a dataset for use by the GSM.

Note that this ingest command can do multiple imports at the same time. By just
adding more names/files to the command. For example: ``gsm-import gleam racs <csv file>``.
And each ingest will run one after the other.

The ingest process does not create the ``catalogue.yaml`` which is required for
retrieving more details from the API.

Currently 3 types of datasets can be created:

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


Persisting a Dataset
--------------------

This section will describe a possible way to persist a dataset for an instance
that is not running yet.

Once new data has been ingested (using `Creating a Dataset`_) compress the directory using the following command:

.. code-block:: bash

    $ cd ${GSM_DATA}
    $ tar cf - "<directory>" | pigz -9 > "<directory (without spaces)>_$(date "+%Y%m%d").tar.gz"


Downlading an Existing Dataset
------------------------------

This section will describe how datasets can be loaded into the GSM, either on startup
or into an existing running instance.

On Application Startup
~~~~~~~~~~~~~~~~~~~~~~

On startup there are 2 environment variables that is used to determine which
datasets to download and prepare for use.

* ``TMDATA_SOURCE`` : is an optional environment variable that can be set to a path
  that can be used for the telescope model source data. If blank the system
  will look in the default list of sources.
* ``TMDATA_KEYS`` : is an optional environment variable which should contain a comma seperated list
  of keys that should be downloaded on startup. The GSM system assumes that these files are considered
  as large files, and as such will download the listed file. These files should be ``.tar.gz`` compressed
  files created in the `Downlading an Existing Dataset`_ section

On an Existing Instance
~~~~~~~~~~~~~~~~~~~~~~~

On a running instance you can manually load more datasets. Data sets should be in ``.tar.gz``
for the download process to work.

There are 2 methods for adding a new dataset. Either my copying the Tar Gzip file to the instance,
or having the file mentioned in a TMData source.

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

Metadata file
-------------

Within each dataset a metadata file is used. This file is technically optional.
But without it the search interface won't give many details.

An example of what the file should look like is as follows:

.. code-block:: yaml

    interface: http://schema.skao.int/catalogue-meta/0.1

    name: RACS

    version: 1

    context:
      date: 10-11-2024
      description: "ASKAP Racs Catalogue"
      notes: "Mid and low frequency catalog"

    config:
      default-attributes:
        - "name"
        - "RAJ2000"
        - "DEJ2000"
      attributes:
        - "recno"
        - "name"
        - "RAJ2000"
        - "DEJ2000"