
SDP Global Sky Model Overview
=============================

The SDP Global Sky Model (GSM) is a service used to manage and obtain sky models.
Subsets of the Global Sky Model are used in SKA pipelines to bootstrap the
calibration process. These subsets are known as Local Sky Models (LSM).

This user guide focuses on the two main users of this service:

- Operations users will create and update new versions of the Global Sky Model.
- Pipeline developers will request a Local Sky Model, which will be
  written to a file, and ingested by the pipeline when it runs.

For developers, please refer to the :doc:`Developer Guide </developerguide/Development>`.


How to upload data to the Global Sky Model
------------------------------------------

Uploading a new version of the GSM catalogue can be done through the user
interface or as a bulk upload.

For instructions for using the bulk upload method, please see the
:doc:`Batch Upload Guide </userguide/batch_upload>`.

GSM file format for upload
^^^^^^^^^^^^^^^^^^^^^^^^^^
The CSV file format that should be used when submitting an updated version
of the GSM catalogue is described in the section at :ref:`CSV File Format`.


How to request a Local Sky Model
--------------------------------

Request LSM via a browser
^^^^^^^^^^^^^^^^^^^^^^^^^
The typical use case for requesting an LSM is from the pipeline as a file.
An operations user can review an LSM request in the browser.
This is meant for quality control only.
For details, go to :doc:`Viewing data in a browser </userguide/viewing_data_in_browser>`.


Request LSM file for a pipeline
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The process by which pipelines should request a Local Sky Model (LSM) is
described on the page :doc:`requesting_a_lsm`


LSM file contents
^^^^^^^^^^^^^^^^^
The CSV file format used to save the LSM data is described at
:doc:`lsm_file_structure`.
