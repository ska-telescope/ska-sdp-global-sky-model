Quick view
==========

This user guide focuses on the two main users of this service:

- Operations users will create and update new versions of the Global Sky Model.
- Pipeline developers will request a Local Sky Model, which will be
  written to a file, and ingested by the pipeline when it runs.

For developers, please refer to the :ref:`Developer guide <>`.

How to request a Local Sky Model
--------------------------------

The process by which pipelines should request a Local Sky Model (LSM) is
described on the page :ref:`request_lsm_processing`.

The CSV file format used to save the LSM data is described in the
design sections: :ref:`lsm_file`.

In addition, we provide an API to easily review a subset of the GSM data
in the browser, which is described at :ref:`lsm_browser`.


How to upload data to the Global Sky Model
------------------------------------------

A browser interface and corresponding API are provided for uploading
catalogue data in the form of CSV files. See: :ref:`batch_upload`.

