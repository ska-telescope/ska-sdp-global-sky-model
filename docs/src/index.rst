SDP Global Sky Model
====================

The SKA SDP Global Sky Model repository contains the code for the SKA global sky model (GSM), which is resource which maps out radio sources.
It works as a service that ingests observation data and can be used by SDP or other users to fetch local sky model data.
The main use case for this model is to aid calibration efforts.

Initially, the project will not have observation data collected from SKA observations.
As such, an existing catalog is used to bootstrap the SKA Global Sky Model.
For the purposes of the LOW telescope, the selected catalog is the `GLEAM catalog <https://www.mwatelescope.org/science/galactic-science/gleam/>`_.
A catalog for the MID telescope is the `RACS catalog <https://www.atnf.csiro.au/research/RACS/RACS_I1/>>`_.
Once higher resolution observations are obtained, the SKA data will be used to update and improve the GSM.

The GSM service uses `Fast API <https://fastapi.tiangolo.com>`_ as the main API framework and `starlette <https://www.starlette.io>`_
to set up configurations. It also makes use of a data store implemented with `PostgreSQL <https://www.postgresql.org>`_.
`HEALPix <https://healpix.sourceforge.io>`_ is used for sky-coordinate calculations.

If you just want to use the GSM service, check the `User Guide <userguide/overview.html>`_.
To understand the internals, check out the `Developer Guide <developerguide/Development.html>`_.

.. toctree::
  :maxdepth: 1
  :caption: User Guide

  userguide/overview
  userguide/api_use
  userguide/sky_model_datasets

.. toctree::
  :maxdepth: 1
  :caption: Developer Guide

  developerguide/Development
  developerguide/Deployment

.. toctree::
  :maxdepth: 1
  :caption: Releases

  changelog
