Sky Model Dataset
=================

This section describes how the GSM handles datasets and what metadata files are needed. For
information on how to ingest, create and upload new datasets for the the GSM Service, please
see :ref:`get_data`.

Initially, the model will not be based on data collected from SKA observations.
As such, existing catalogues are used to bootstrap the SKA Global Sky Model.
Once higher resolution observations are obtained, the SKA data will be used to update and improve the GSM.

Existing Datasets
-----------------

There are two datasets that are available for use:

  - `GLEAM <https://www.mwatelescope.org/science/galactic-science/gleam/>`_ (Murchison Widefield Array):
    to bootstrap the Low telescope sky model.
  - `RACS <https://www.atnf.csiro.au/research/RACS/RACS_I1/>`_ (Australian Square Kilometre Array Pathfinder)
    to bootstrap the Mid telescope sky model.


.. _metadata:

Metadata file
-------------

Each dataset should use a metadata file called ``catalogue.yaml``.
This describes the dataset and is needed to correctly interpret the catalogue data.
Without the catalogue, only basic information like the ``Heal_Pix_position`` is returned by the API.

It should live in the directory containing the catalogue data files and will allow more attributes
to be returned by the API, as well as metadata for the catalogue.

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
