SDP Global Sky Model
====================

The SKA SDP Global Sky Model (GSM) is a resource for indexing and querying radio source catalogues.
It works as a service that ingests observation data and can be used by SDP or other users to fetch local sky model data.
The main use case for this model is to aid calibration efforts.

The GSM service provides an API to query sources based on their coordinates,
flux density and catalogue. The catalogue data is stored in a file based
format which uses `HEALPix <https://healpix.sourceforge.io>`_ tiles.

.. toctree::
  :maxdepth: 1
  :caption: Design

  design/overview
  design/sky-model-datasets

.. toctree::
  :maxdepth: 1
  :caption: User Guide

  user/api-use
  user/get-data
  user/cli-use

.. toctree::
  :maxdepth: 1
  :caption: Developer Guide

  developer/development
  developer/deployment

.. toctree::
  :maxdepth: 1
  :caption: Releases

  changelog
