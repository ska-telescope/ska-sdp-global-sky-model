Overview
========

The intended design of the GSM can be found in
`Solution Intent <https://confluence.skatelescope.org/display/SWSI/SDP+Global+Sky+Model>`_.
The most up-to-date implementation is described here.

Purpose
-------

The GSM service provides a subset of sky model data (i.e. Local Sky Model, LSM) that
are used in SKA pipelines to bootstrap the calibration process.

The GSM is a catalogue of different data sets, eventually fully created from
SKA observations. It contains fitted model parameters for source components
as measured and processed by SKA scientists.

Users
-----

The GSM has two main user-base:

- SDP processing pipelines: SDP processing scripts request an LSM for every
  pipeline that needs it for every field that is processed. The pipelines
  use the resulting LSM as needed.

- Commissioning scientists and science operators, who create the sky model data
  from SKA sky surveys and maintain the GSM data base with the latest datasets.

Design
------

The database design is described at :ref:`db_design`.

The two main functionality of the GSM is also described:

- :ref:`request_design`
- :ref:`upload_design`
