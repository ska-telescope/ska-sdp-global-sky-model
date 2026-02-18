SDP Global Sky Model Service
============================

The SKA SDP Global Sky Model (GSM) service is a service within the
`Science Data Processor (SDP) <https://developer.skao.int/projects/ska-sdp-integration/en/latest/index.html>`_
to provide sky model data for data processing pipelines. The model is developed by
users (commissioning scientists and science oprators performing SKA global sky surveys)
and uploaded into the GSM database for use.

Main features:

- Request an LSM (Local Sky Model) for SDP processing pipelines:

    - via the SDP architecture (using processing scripts)
    - via the GSM API

- Uploading a new GSM version into the database

The `ska-sdp-global-sky-model <https://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model>`_
repository contains the code for the GSM service.

.. toctree::
  :maxdepth: 1
  :caption: Design

  design/overview
  design/database
  design/request-lsm
  design/upload-gsm

.. toctree::
  :maxdepth: 1
  :caption: User Guide

  userguide/overview
  userguide/batch-upload
  userguide/request-lsm

.. toctree::
  :maxdepth: 1
  :caption: Developer Guide

  developerguide/Development
  developerguide/Deployment
  developerguide/Database

.. toctree::
  :maxdepth: 1
  :caption: Releases

  changelog
