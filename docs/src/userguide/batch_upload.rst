Uploading Sky Survey Data
=========================

The SKA Global Sky Model (GSM) provides both a browser interface and API endpoints for uploading multiple sky survey catalog files in a single atomic batch operation into the GSM database.

Overview
--------

The batch upload feature allows you to:

- Upload multiple CSV files simultaneously via API or browser interface (all files are combined into a single sky model)
- Stage uploads for review before committing to the main database
- Track upload progress with a unique identifier
- Query upload status and errors
- Ensure atomic ingestion (all files succeed or none are ingested)
- Process uploads asynchronously in the background for optimal API responsiveness
- Automatic data validation at the schema level after CSV transformation
- Version control with semantic versioning for updated components

Browser Upload Interface
~~~~~~~~~~~~~~~~~~~~~~~~

A browser interface is available at the root URL (``http://localhost:8000/``). The interface provides:

- **Drag-and-drop file upload**: Simply drag CSV files onto the upload zone
- **Multiple file selection**: Upload multiple catalogs simultaneously
- **Real-time status monitoring**: Track upload progress with automatic polling
- **Staging table preview**: Review uploaded data before committing
- **Commit/Reject workflow**: Approve or discard staged uploads
- **Version information**: Visual indicators for versioning behavior

**Using the Browser Interface**:

1. Navigate to ``http://localhost:8000/`` in your web browser
2. Drag and drop CSV files onto the upload zone (or click to browse)
3. Click "Upload Files" to begin the upload
4. Monitor the upload progress - status updates automatically
5. Review the staged data including a sample preview
6. Click "Commit to Database" to approve or "Reject and Discard" to cancel

The browser interface automatically handles:
    - File validation (CSV format only)
    - Upload tracking with unique IDs
    - Status polling every 2 seconds
    - Error display if uploads fail

Staging and Versioning Workflow
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Two-Stage Upload Process**:

All uploads now use a staging workflow for safety and review:

1. **Upload to Staging**: Files are first uploaded to ``sky_component_staging`` table
2. **Review Data**: Use ``/review-upload/{upload_id}`` to inspect a sample of staged data
3. **Commit or Reject**: 
   - Commit: Move data to main table with automatic versioning
   - Reject: Discard all staged data for that upload

**Automatic Versioning**:

When committing staged data, the system automatically handles versioning:

- **New components**: Start at version ``0.0.0``
- **Updated components**: Increment minor version (e.g., ``0.0.0`` → ``0.1.0``, ``0.1.0`` → ``0.2.0``)
- **Version tracking**: Each component_id can have multiple versions
- **Unique constraint**: ``component_id + version`` must be unique

This allows you to:
    - Update existing sky sources without losing historical data
    - Track changes to sources over time
    - Query specific versions or always get the latest version

**Staging Table Schema**:

The ``sky_component_staging`` table mirrors the main ``sky_component`` table but includes:
    - ``upload_id``: Links all sources in a batch upload
    - Unique constraint: ``component_id + upload_id`` (allows same source in different uploads)

Asynchronous Processing
~~~~~~~~~~~~~~~~~~~~~~~~

Batch uploads run asynchronously as background tasks. When you submit files:

1. Files are validated for format (CSV, proper MIME type)
2. Upload is created with a unique ID and status "uploading"
3. API returns immediately with the upload ID
4. Data is ingested to staging table in the background
5. Query the status endpoint to monitor progress
6. Once complete, review and commit (or reject) the staged data

This design keeps the API responsive during large uploads and allows multiple concurrent batch operations.

CSV File Format
---------------

The uploaded CSV files must follow the standardized sky survey catalog format compatible with the 
`ska_sdp_datamodels <https://gitlab.com/ska-telescope/sdp/ska-sdp-datamodels/-/blob/main/src/ska_sdp_datamodels/global_sky_model/global_sky_model.py?ref_type=heads>`_ package. CSV files should use the standardized column names.

Required Columns
~~~~~~~~~~~~~~~~

Your CSV must include these standardized columns:

- **component_id**: Unique component identifier (string)
- **ra**: Right ascension (J2000) in degrees (float)
- **dec**: Declination (J2000) in degrees (float)
- **i_pol**: I polarization flux at reference frequency in Janskys (float)

Optional Columns
~~~~~~~~~~~~~~~~

Additional standardized columns that will be automatically ingested if present:

**Source Shape (Gaussian Model)**:
    - ``major_ax``: Major axis in arcseconds (float)
    - ``minor_ax``: Minor axis in arcseconds (float)
    - ``pos_ang``: Position angle in degrees (float)

**Spectral Properties**:
    - ``spec_idx``: Spectral index (float or array of polynomial coefficients)
    - ``log_spec_idx``: Boolean flag for logarithmic spectral model (bool)

**Polarization**:
    - ``q_pol``: Q polarization flux in Jy (float)
    - ``u_pol``: U polarization flux in Jy (float)
    - ``v_pol``: V polarization flux in Jy (float)
    - ``pol_frac``: Polarized fraction (float)
    - ``pol_ang``: Polarization angle in radians (float)
    - ``rot_meas``: Faraday rotation measure in rad/m² (float)

CSV Format Examples
~~~~~~~~~~~~~~~~~~~~

**Standardized Format**:

The ``test_catalog_1.csv`` and ``test_catalog_2.csv`` files in the test data directory demonstrate 
the required standardized format:

.. code-block:: text

    component_id,ra,dec,i_pol,major_ax,minor_ax,pos_ang,spec_idx,log_spec_idx
    J025837+035057,44.656883,3.849425,0.835419,142.417,132.7302,3.451346,-0.419238,False
    J030420+022029,46.084633,2.341634,0.29086,137.107,134.2583,-0.666618,-1.074094,False

These test catalogs contain 100 components each and are used throughout the test suite as reference examples.

**Minimal Format**:

At minimum, you need the four required columns:

.. code-block:: text

    component_id,ra,dec,i_pol
    J000001-350001,0.004,-35.0,0.25
    J000002-350002,0.008,-35.1,0.23

Data Validation
~~~~~~~~~~~~~~~~

**Important**: The API performs only basic technical validation (data types, required fields, coordinate ranges). 
No scientific validation is performed - users are responsible for ensuring their data is scientifically accurate, including correct flux values, proper component positions, and appropriate units.

After CSV files are loaded, each component undergoes validation. The following checks are performed:

**Field Validation**:

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

**Validation Process**:
    - All components are validated before any data is ingested
    - Validation errors are collected and logged for all components
    - If ANY validation errors occur, NO data is ingested (all-or-nothing)
    - Only if ALL components pass validation will ingestion proceed

Best Practices
--------------

1. **Data Preparation**: Verify the scientific accuracy of your data before upload. The API does not validate 
   flux values, component identifications, or other scientific properties - only basic technical requirements.

2. **File Validation**: Ensure all CSV files are properly formatted with standardized column names before upload 
   to avoid batch failures. Required columns are component_id, ra, dec, and i_pol.

3. **Progress Monitoring**: Use the status endpoint to monitor long-running uploads, especially for large batches.

4. **Error Handling**: Always check the response status code and handle errors appropriately in your application.

5. **Batch Size**: Consider breaking very large uploads into smaller batches for better manageability.

API Endpoints
-------------

Upload Sky Survey Batch
~~~~~~~~~~~~~~~~~~~~~~~~

**Endpoint**: ``POST /upload-sky-survey-batch``

Upload and ingest one or more sky survey CSV files to the staging table. All files in the batch are combined into a single sky model. All files are validated and uploaded to a staging area before ingestion begins. If any file fails validation or ingestion, the entire batch is rolled back.

**Parameters**:

.. list-table::
    :widths: 20, 50, 20, 10
    :header-rows: 1

    * - Parameter
      - Description
      - Data Type
      - Required
    * - ``files``
      - One or more CSV files containing standardized sky survey data
      - list[File]
      - Yes

**Response**:

.. code-block:: json

    {
        "upload_id": "550e8400-e29b-41d4-a716-446655440000",
        "status": "uploading"
    }

**Note**: The endpoint returns immediately with status "uploading". Ingestion to staging table proceeds 
asynchronously in the background. Use the status endpoint to monitor completion, then review and commit.

**Example Usage**:

.. code-block:: bash

    # Upload one or more CSV files with standardized column names
    curl -X POST "http://localhost:8000/upload-sky-survey-batch" \\
      -F "files=@test_catalog_1.csv;type=text/csv" \\
      -F "files=@test_catalog_2.csv;type=text/csv"

**Python Example**:

.. code-block:: python

    import requests
    import time

    url = "http://localhost:8000/upload-sky-survey-batch"
    
    # Upload multiple CSV files with standardized column names
    files = [
        ("files", ("test_catalog_1.csv", open("test_catalog_1.csv", "rb"), "text/csv")),
        ("files", ("test_catalog_2.csv", open("test_catalog_2.csv", "rb"), "text/csv")),
    ]
    response = requests.post(url, files=files)
    
    result = response.json()
    print(f"Upload ID: {result['upload_id']}")
    print(f"Status: {result['status']}")  # Will be "uploading"
    
    # Poll for completion
    status_url = f"{url.replace('/upload-sky-survey-batch', '')}/upload-sky-survey-status/{result['upload_id']}"
    while True:
        status_response = requests.get(status_url)
        status_data = status_response.json()
        if status_data['state'] in ['completed', 'failed']:
            break
        time.sleep(2)
    
    print(f"Final status: {status_data['state']}")

Get Upload Status
~~~~~~~~~~~~~~~~~

**Endpoint**: ``GET /upload-sky-survey-status/{upload_id}``

Retrieve the current status of a sky survey batch upload.

**Parameters**:

.. list-table::
    :widths: 20, 50, 20, 10
    :header-rows: 1

    * - Parameter
      - Description
      - Data Type
      - Required
    * - ``upload_id``
      - Unique identifier returned when the upload was initiated
      - string (UUID)
      - Yes

**Response**:

.. code-block:: json

    {
        "upload_id": "550e8400-e29b-41d4-a716-446655440000",
        "state": "completed",
        "total_files": 3,
        "uploaded_files": 3,
        "remaining_files": 0,
        "errors": []
    }

**Upload States**:

- ``pending``: Upload created but not started
- ``uploading``: Files are being uploaded and validated
- ``completed``: All files uploaded and ingested successfully
- ``failed``: Upload failed (see ``errors`` field for details)

**Example Usage**:

.. code-block:: bash

    curl "http://localhost:8000/upload-sky-survey-status/550e8400-e29b-41d4-a716-446655440000"

**Python Example**:

.. code-block:: python

    import requests
    import time

    upload_id = "550e8400-e29b-41d4-a716-446655440000"
    url = f"http://localhost:8000/upload-sky-survey-status/{upload_id}"
    
    while True:
        response = requests.get(url)
        status = response.json()
        
        print(f"State: {status['state']}")
        print(f"Progress: {status['uploaded_files']}/{status['total_files']}")
        
        if status['state'] in ['completed', 'failed']:
            break
        
        time.sleep(2)
    
    if status['state'] == 'failed':
        print(f"Errors: {status['errors']}")

Review Staged Upload
~~~~~~~~~~~~~~~~~~~~

**Endpoint**: ``GET /review-upload/{upload_id}``

Review staged data before committing to the main database. Returns a summary and sample of the staged records.

**Parameters**:

.. list-table::
    :widths: 20, 50, 20, 10
    :header-rows: 1

    * - Parameter
      - Description
      - Data Type
      - Required
    * - ``upload_id``
      - Unique identifier returned when the upload was initiated
      - string (UUID)
      - Yes

**Response**:

.. code-block:: json

    {
        "upload_id": "550e8400-e29b-41d4-a716-446655440000",
        "total_records": 200,
        "sample": [
            {
                "component_id": "J025837+035057",
                "ra": 0.7793,
                "dec": 0.0672,
                "i_pol": 0.8354,
                "version": null
            }
        ]
    }

**Example Usage**:

.. code-block:: bash

    curl "http://localhost:8000/review-upload/550e8400-e29b-41d4-a716-446655440000"

**Python Example**:

.. code-block:: python

    import requests

    upload_id = "550e8400-e29b-41d4-a716-446655440000"
    url = f"http://localhost:8000/review-upload/{upload_id}"
    
    response = requests.get(url)
    review = response.json()
    
    print(f"Total records: {review['total_records']}")
    print(f"Sample data: {review['sample'][:3]}")  # First 3 records

Commit Staged Upload
~~~~~~~~~~~~~~~~~~~~

**Endpoint**: ``POST /commit-upload/{upload_id}``

Commit staged data to the main database with automatic versioning. New components get version ``0.0.0``, 
existing components get their minor version incremented.

**Parameters**:

.. list-table::
    :widths: 20, 50, 20, 10
    :header-rows: 1

    * - Parameter
      - Description
      - Data Type
      - Required
    * - ``upload_id``
      - Unique identifier of the staged upload to commit
      - string (UUID)
      - Yes

**Response**:

.. code-block:: json

    {
        "message": "Upload committed successfully",
        "records_committed": 200,
        "upload_id": "550e8400-e29b-41d4-a716-446655440000"
    }

**Example Usage**:

.. code-block:: bash

    curl -X POST "http://localhost:8000/commit-upload/550e8400-e29b-41d4-a716-446655440000"

**Python Example**:

.. code-block:: python

    import requests

    upload_id = "550e8400-e29b-41d4-a716-446655440000"
    url = f"http://localhost:8000/commit-upload/{upload_id}"
    
    response = requests.post(url)
    result = response.json()
    
    print(f"Committed {result['records_committed']} records")
    print(f"Message: {result['message']}")

Reject Staged Upload
~~~~~~~~~~~~~~~~~~~~~

**Endpoint**: ``DELETE /reject-upload/{upload_id}``

Reject and discard staged data. All records associated with this upload_id are permanently deleted from the staging table.

**Parameters**:

.. list-table::
    :widths: 20, 50, 20, 10
    :header-rows: 1

    * - Parameter
      - Description
      - Data Type
      - Required
    * - ``upload_id``
      - Unique identifier of the staged upload to reject
      - string (UUID)
      - Yes

**Response**:

.. code-block:: json

    {
        "message": "Upload rejected successfully",
        "records_deleted": 200,
        "upload_id": "550e8400-e29b-41d4-a716-446655440000"
    }

**Example Usage**:

.. code-block:: bash

    curl -X DELETE "http://localhost:8000/reject-upload/550e8400-e29b-41d4-a716-446655440000"

**Python Example**:

.. code-block:: python

    import requests

    upload_id = "550e8400-e29b-41d4-a716-446655440000"
    url = f"http://localhost:8000/reject-upload/{upload_id}"
    
    response = requests.delete(url)
    result = response.json()
    
    print(f"Rejected and deleted {result['records_deleted']} records")
    print(f"Message: {result['message']}")

CSV File Format
---------------

The uploaded CSV files should follow the sky survey catalog format compatible with the GlobalSkyModel schema. 
The API automatically maps common column names to the standardized schema fields.

Required Columns
~~~~~~~~~~~~~~~~

At minimum, your CSV must include:

- **Source identifier**: Column specified by the ``source`` field in config (e.g., ``GLEAM``, ``SOURCE_ID``)
- **RAJ2000**: Right ascension (J2000) in degrees
- **DEJ2000**: Declination (J2000) in degrees  
- **Flux measurement**: At least one of ``Fpwide``, ``Fintwide``, or ``i_pol`` (Stokes I polarization flux in Jy)

Optional Columns
~~~~~~~~~~~~~~~~

Additional columns that will be automatically ingested if present:

**Source Shape (Gaussian Model)**:
    - ``awide`` or ``major_ax``: Major axis in arcseconds
    - ``bwide`` or ``minor_ax``: Minor axis in arcseconds
    - ``pawide`` or ``pos_ang``: Position angle in degrees

**Spectral Properties**:
    - ``alpha`` or ``spec_idx``: Spectral index (or array of polynomial coefficients)
    - ``spec_curv``: Spectral curvature parameter
    - ``log_spec_idx``: Boolean flag for logarithmic spectral model

**Polarization**:
    - ``q_pol``: Stokes Q flux in Jy
    - ``u_pol``: Stokes U flux in Jy
    - ``v_pol``: Stokes V flux in Jy
    - ``pol_frac``: Polarized fraction
    - ``pol_ang``: Polarization angle in radians
    - ``rot_meas``: Faraday rotation measure in rad/m²

CSV Format Examples
~~~~~~~~~~~~~~~~~~~~

**GLEAM Format** (Full catalog with frequency bands):

.. code-block:: text

    recno,GLEAM,RAJ2000,DEJ2000,Fpwide,Fintwide,awide,bwide,pawide,alpha
    1,J000001-350001,0.004,-35.0,0.25,0.26,170,140,-6.2,-0.527
    2,J000002-350002,0.008,-35.1,0.23,0.24,168,138,-6.0,-0.534

**Standardized Format** (used by test catalogs):

The ``test_catalog_1.csv`` and ``test_catalog_2.csv`` files in the test data directory demonstrate 
the standardized format with explicit column names:

.. code-block:: text

    component_id,ra,dec,i_pol,major_ax,minor_ax,pos_ang,spec_idx,log_spec_idx
    J025837+035057,44.656883,3.849425,0.835419,142.417,132.7302,3.451346,-0.419238,False
    J030420+022029,46.084633,2.341634,0.29086,137.107,134.2583,-0.666618,-1.074094,False

These test catalogs contain 100 sources each derived from GLEAM data and are used throughout 
the test suite as reference examples.

Custom Schema Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If your CSV uses different column names, provide a custom configuration with ``heading_alias`` mappings:

.. code-block:: python

    custom_config = {
        "ingest": {
            "wideband": True,
            "agent": "file",
            "file_location": [{
                "key": "unset",
                "heading_alias": {
                    "RA": "RAJ2000",           # Map your RA column
                    "DEC": "DEJ2000",          # Map your Dec column
                    "FLUX_I": "i_pol",         # Map flux column
                    "SOURCE_NAME": "GLEAM",     # Map source ID column
                },
                "heading_missing": [],
                "bands": [],
            }]
        },
        "name": "My Custom Survey",
        "catalog_name": "CUSTOM",
        "frequency_min": 80,
        "frequency_max": 300,
        "source": "SOURCE_NAME",  # Column name for source identifier
        "bands": [],
    }

Note: Passing custom configurations via curl is not supported due to multipart form-data limitations. 
Use Python or update the default DEFAULT_CATALOG_CONFIG in ``config.py``.

Error Handling
--------------

The API provides detailed error responses for common issues:

**Invalid File Type** (400):

.. code-block:: json

    {
        "detail": "Invalid file type for survey.txt. Must be CSV."
    }

**Database Connection Error** (500):

.. code-block:: json

    {
        "detail": "Unable to access database"
    }

**Ingestion Failure** (500):

.. code-block:: json

    {
        "detail": "Sky survey upload failed: Ingest failed for test_catalog_1.csv"
    }

**Upload Not Found** (404):

.. code-block:: json

    {
        "detail": "Upload ID not found"
    }

Best Practices
--------------

1. **File Validation**: Ensure all CSV files are properly formatted before upload to avoid batch failures. 
   Required columns are source identifier, RAJ2000, DEJ2000, and at least one flux measurement.

2. **Progress Monitoring**: Use the status endpoint to monitor long-running uploads, especially for large batches.

3. **Review Before Commit**: Always review staged data using ``/review-upload/{upload_id}`` before committing to ensure data quality.

4. **Versioning Awareness**: Understand that committing data with existing component_ids will create new versions, not replace existing data.

5. **Error Handling**: Always check the response status code and handle errors appropriately in your application.

6. **Batch Size**: Consider breaking very large uploads into smaller batches for better manageability.

7. **Column Naming**: Use standard GLEAM-style column names (RAJ2000, DEJ2000, Fpwide, etc.) for automatic mapping, 
   or provide custom ``heading_alias`` mappings in your configuration.

8. **Cleanup**: Both rejected and committed uploads are removed from the staging table.

Data Model Schema
-----------------

The batch upload system conforms to the ``SkySource`` dataclass from ``ska_sdp_datamodels.global_sky_model``. 
All uploaded sources are stored with the following schema:

**Core Fields** (at least name, ra, dec, i_pol required):
    - ``name``: Source identifier (string)
    - ``ra``: Right ascension in radians (float)
    - ``dec``: Declination in radians (float)
    - ``i_pol``: Stokes I flux in Janskys (float)

**Gaussian Model** (optional):
    - ``major_ax``: Semi-major axis in radians (float)
    - ``minor_ax``: Semi-minor axis in radians (float)  
    - ``pos_ang``: Position angle in radians (float)

**Spectral Features** (optional):
    - ``spec_idx``: List of up to 5 spectral index polynomial coefficients (list[float])
    - ``log_spec_idx``: Logarithmic spectral model flag (bool)
    - ``spec_curv``: Spectral curvature parameter (float)

**Polarization** (optional):
    - ``q_pol``, ``u_pol``, ``v_pol``: Stokes parameters in Jy (float)
    - ``pol_frac``: Polarized fraction (float)
    - ``pol_ang``: Polarization angle in radians (float)
    - ``rot_meas``: Faraday rotation measure in rad/m² (float)

**Database-Specific**:
    - ``healpix_index``: HEALPix index for spatial indexing (computed automatically)

Architecture
------------

The batch upload system uses a clean separation of concerns:

- **Upload Manager** (``upload_manager.py``): Handles file storage, file type validation, and state tracking
- **Main API** (``main.py``): Provides HTTP endpoints, coordinates workflow, manages background tasks, and handles staging/commit/reject operations
- **Ingest Module** (``ingest.py``): Processes CSV data, validates against schema, and imports into staging or main database
- **Models** (``models.py``): Defines database schema including ``SkyComponent`` (main table with versioning) and ``SkyComponentStaging`` (staging table)

Key Features:

- **Two-Stage Workflow**: Upload to staging → review → commit or reject
- **Automatic Versioning**: Semantic versioning applied during commit (0.0.0, 0.1.0, 0.2.0, ...)
- **Browser Interface**: HTML interface for interactive uploads
- **Asynchronous Processing**: Background tasks via FastAPI BackgroundTasks keep API responsive
- **Modern Lifecycle Management**: Uses FastAPI lifespan context manager (not deprecated on_event)
- **Schema-Level Validation**: Data validated after transformation to standardized format
- **Clean Temporary Storage**: Temporary files are always cleaned up, even on errors
- **State Tracking**: Upload status persists across the upload lifecycle
- **Error Isolation**: Failures are captured and reported without affecting other uploads

Validation Pipeline:

1. File type validation (CSV format, MIME type)
2. CSV parsing and transformation to schema format
3. Source-by-source validation (required fields, ranges, types)
4. Batch insertion to staging table
5. User review of staged data
6. Commit with version assignment or reject to discard
7. Cleanup and status update
