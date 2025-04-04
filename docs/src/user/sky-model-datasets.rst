Sky Model Dataset
=================

This section will describe how the GSM handles datasets and what metadata files are needed. For 
information on how to how to ingest, create and upload new datasets for the the GSM Service, please
see the :doc:`CLI docs </cli-use>`.

Existing Datasets
-----------------

There are two datasets that are available for use:

  - Murchison Widefield Array (GLEAM)
  - ASKAP (RACS)

Downloading the Data
--------------------

You can use the CLI for this, or, see below for other options.

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

  - Finish ingestion (using `Creating a New Dataset`_)
  - Compress the directory using the following command:

.. code-block:: bash

    $ cd ${GSM_DATA}
    $ tar cf - "<directory>" | pigz -9 > "<directory (without spaces)>_$(date "+%Y%m%d").tar.gz"


Metadata file
-------------

Each dataset should use a metadata file called ``catalogue.yaml``.
This describes the dataset and is needed to correctly interpret the catalogue data.
Without the catalogue only basic information like the Heal_Pix_position is returned by the API.

It should live in the ``/datasets`` directory and will allow more attributes to be returned by the API, as well as metadata for the catalogue.

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

Now, when queried, the API will return ``RAJ2000`` and ``DEJ2000`` columns alongside the HEALPix information.

``default_attributes`` are the columns which will be automatically returned by the API when /local_sky_model is queried, and ``attributes`` are the columns that can be filtered on by a query.
