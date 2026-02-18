.. _request_design:

Requesting a Local Sky Model
----------------------------

The main purpose of the GSM is to provide a service for requesting
LSM data for processing pipelines to use. Alternatively, the service
also provides an API (see :ref:``) to obtain the data. The following flow chart
describes the two paths by which one can request an LSM.

.. figure:: ../images/lsm-flow.png

.. _sdp_request:

SDP Processing
^^^^^^^^^^^^^^

In the SDP, processing scripts request the deployment of various processing
pipeliens, including, e.g. the realtime calibartion pipeline, the instrumental
calibration pipeline or the iterative calibration pipeline. All of these
pipeliens require an LSM to be available for their calibration
processes to run with high accuracy.

The process to request an LSM follows the `SDP architecutre <https://developer.skao.int/projects/ska-sdp-integration/en/latest/index.html>`_.
Processing scripts specify the request paramertes via data flow entites (see `ADR-81 <https://confluence.skatelescope.org/display/SWSI/ADR-81+Centralise+SDP+data+product%2C+queue+and+Tango+configuration+as+data+flows>`_),
which are stored in the SDP Configuration database (Config DB).

The GSM Service continously monitors the Config DB for new LSM requests, and
when it finds one, it extracts the query parameters, then queries the PostgreSQL
database using !!!HealPix (Qc3)!!!!. The returned data are then stored in a
CSV file (one per LSM request) in a location that is specified by the data flow entity
and is accessible to both the GSM service and the processing pipelines.

The user guide provides examples of how one can set up their processing scripts
for this process and what the GSM requires in its query parameters: :ref:<>.

LSM file contents
^^^^^^^^^^^^^^^^^

The CSV file format used to save the LSM data is described at
:ref:`lsm_file`.