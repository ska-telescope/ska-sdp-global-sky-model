.. _upload_design:

Uploading a new version of GSM data
-----------------------------------

The GSM provides both a browser interface
and API endpoints for uploading multiple sky survey catalogue files
in a single atomic batch operation into the GSM database.

The process allows the following:

- Provide catalogue metadata via JSON file (required - includes version, name, description, reference frequency, and epoch)
- Upload multiple CSV files simultaneously via API or browser interface.
- CSV files uploaded in a single upload session will be part of the same catalogue version.
- Track upload progress with a unique identifier.
- Query upload status and errors.
- Review the last few entries of the uploaded data, then manually
  commit or reject the upload.
- Ensure atomic ingestion (all files succeed or none are ingested).
- Automatic data validation at the schema level.
- Version control with semantic versioning of the catalogue.

Batch uploads run asynchronously as background tasks. This design keeps the API responsive
during large uploads and allows multiple concurrent batch operations.

A detailed user guide of the browser interface and the API can be
found at :ref:`batch_upload`.

Staging and versioning of data
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Two-stage upload process
........................

All uploads stage the data into a ``staging table`` first, which follows the schema of the
main tables, except it also includes an ``upload_id`` to distinguish between different
upload sessions.

Once the user ``commits`` the data, they are moved from the staging table into the
main table. When the upload is ``rejected``, the data are removed form the staging
table and are not moved to the main one.

Catalogue Metadata File
.......................

Every upload must include a ``metadata.json`` file containing catalogue-level information that applies 
to all components in the catalogue. The metadata follows the ``GlobalSkyModelMetadata`` dataclass format 
from `ska_sdp_datamodels package <https://gitlab.com/ska-telescope/sdp/ska-sdp-datamodels/-/blob/main/src/ska_sdp_datamodels/global_sky_model/global_sky_model.py>`_ with a few additional fields annotating the catalogue.

**Metadata File Format**:

.. code-block:: json

    {
      "version": "1.0.0",
      "catalogue_name": "GLEAM",
      "description": "GaLactic and Extragalactic All-sky MWA Survey",
      "ref_freq": 170000000,
      "epoch": "J2000",
      "author": "GLEAM Team",
      "reference": "https://doi.org/10.1093/mnras/stw2337",
      "notes": "170 MHz continuum survey"
    }

**Required Fields**:
    - ``version``: Semantic version (e.g., "1.0.0") - must increment from previous versions
    - ``catalogue_name``: Catalogue identifier (e.g., "GLEAM", "RACS", "RCAL")
    - ``description``: Human-readable description of the catalogue
    - ``ref_freq``: Reference frequency in Hz (float/integer)
    - ``epoch``: Epoch of observation (e.g., "J2000")

**Optional Fields**:
    - ``author``: Author or team name
    - ``reference``: DOI, URL, or citation
    - ``notes``: Additional information

Files uploaded in a new session (new ``upload_id``) will create a new catalogue version with its own version number as per the metadata json file. Uploading a new version requires incrementing the version number in the metadata file to ensure proper version tracking, duplicate version numbers are not allowed.

.. _upload_csv_format:

CSV file format
^^^^^^^^^^^^^^^

The uploaded CSV files must be compatible with the data models defined in the
`ska_sdp_datamodels package <https://gitlab.com/ska-telescope/sdp/ska-sdp-datamodels/-/blob/main/src/ska_sdp_datamodels/global_sky_model/global_sky_model.py>`_.
The columns and data types need to match the models; if a column is not provided,
the default will be loaded into the database.

Required columns:

- ``component_id``: Unique component identifier (string)
- ``ra``: Right ascension (J2000) in degrees (float)
- ``dec``: Declination (J2000) in degrees (float)
- ``i_pol``: I polarization flux at reference frequency in Janskys (float)

Data Validation
^^^^^^^^^^^^^^^

.. note::

    The API performs only basic technical validation (data types, required fields, coordinate ranges).
    No scientific validation is performed - users are responsible for ensuring their data are scientifically accurate.

After CSV files are loaded, each component undergoes validation.
The following checks are performed:

.. list-table::
    :widths: 20, 15, 15, 50
    :header-rows: 1

    * - Field
      - Data Type
      - Required
      - Validation Checks
    * - ``component_id``
      - string
      - Yes
      - Must be present, non-empty, and unique (across all files in batch)
    * - ``ra``
      - float
      - Yes
      - Must be numeric, range: 0 to 360 degrees
    * - ``dec``
      - float
      - Yes
      - Must be numeric, range: -90 to 90 degrees
    * - ``i_pol``
      - float
      - Yes
      - Must be numeric

Each ingested component is validated individually.
If any validation errors occur, no data will be ingested.
Only if all components pass validation will ingestion proceed.
