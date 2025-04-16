SDP Global Sky Model
====================

The SKA SDP Global Sky Model (GSM) is a resource for indexing and querying radio source catalogues.
It works as a service that ingests observation data and can be used by SDP or other users to fetch local sky model data.
The main use case for this model is to aid calibration efforts.

Initially, the project will not have observation data collected from SKA observations.
As such, an existing catalogue is used to bootstrap the SKA Global Sky Model.
For the purposes of the Low telescope, the selected catalogue is the `GLEAM catalogue <https://www.mwatelescope.org/science/galactic-science/gleam/>`_.
A catalogue for the Mid telescope is the `RACS catalogue <https://www.atnf.csiro.au/research/RACS/RACS_I1/>`_.
Once higher resolution observations are obtained, the SKA data will be used to update and improve the GSM.

The GSM service provides an API to query sources based on their coordinates, flux and catalogue. The catalogue data is stored in a file based
format which utilises `HEALPix <https://healpix.sourceforge.io>`_ tiles.


If you just want to use the GSM service, check the `User Guide <user/overview.html>`_.
To understand the internals, check out the `Developer Guide <developer/development.html>`_.

.. toctree::
  :maxdepth: 1
  :caption: User Guide

  user/overview
  user/api-use
  user/sky-model-datasets
  user/cli-use

.. toctree::
  :maxdepth: 1
  :caption: Developer Guide

  developer/development
  developer/deployment

.. toctree::
  :maxdepth: 1
  :caption: Releases

  ../../changelog
