Batch Upload API
================

The SKA Global Sky Model API provides endpoints for uploading multiple sky survey catalog files in a single atomic batch operation.

Overview
--------

The batch upload feature allows you to:

- Upload multiple CSV files simultaneously
- Track upload progress with a unique identifier
- Query upload status and errors
- Ensure atomic ingestion (all files succeed or none are ingested)
- Process uploads asynchronously in the background for optimal API responsiveness
- Automatic data validation at the schema level after CSV transformation

Asynchronous Processing
~~~~~~~~~~~~~~~~~~~~~~~~

Batch uploads run asynchronously as background tasks. When you submit files:

1. Files are validated for format (CSV, proper MIME type)
2. Upload is created with a unique ID and status "uploading"
3. API returns immediately with the upload ID
4. Ingestion proceeds in the background
5. Query the status endpoint to monitor progress

This design keeps the API responsive during large uploads and allows multiple concurrent batch operations.

Data Validation
~~~~~~~~~~~~~~~~

After CSV files are transformed to the standardized database schema, each source undergoes validation:

**Required Fields**:
    - ``name``: Source identifier (string)
    - ``ra``: Right ascension (-360 to 360 degrees, converted to radians)
    - ``dec``: Declination (-90 to 90 degrees, converted to radians)
    - ``i_pol``: Stokes I flux (must be positive, in Janskys)

**Optional Field Validation**:
    - Numeric fields validated for appropriate ranges
    - Invalid sources are logged with warnings and skipped
    - Ingestion stops if more than 100 validation errors occur

This ensures data quality while providing flexibility for varied catalog formats.

API Endpoints
-------------

Upload Sky Survey Batch
~~~~~~~~~~~~~~~~~~~~~~~~

**Endpoint**: ``POST /upload-sky-survey-batch``

Upload and ingest one or more sky survey CSV files atomically. All files are validated and uploaded to a temporary directory before ingestion begins. If any file fails validation or ingestion, the entire batch is rolled back.

**Parameters**:

.. list-table::
    :widths: 20, 50, 20, 10
    :header-rows: 1

    * - Parameter
      - Description
      - Data Type
      - Required
    * - ``files``
      - One or more CSV files containing sky survey data
      - list[File]
      - Yes
    * - ``catalog``
      - Predefined catalog name: 'GLEAM', 'RACS', 'RCAL', or 'GENERIC' (default)
      - string
      - No
    * - ``config``
      - Custom catalog configuration dict (overrides catalog parameter)
      - dict (JSON)
      - No

**Response**:

.. code-block:: json

    {
        "upload_id": "550e8400-e29b-41d4-a716-446655440000",
        "status": "uploading"
    }

**Note**: The endpoint returns immediately with status "uploading". Ingestion proceeds asynchronously 
in the background. Use the status endpoint to monitor completion.

**Example Usage**:

.. code-block:: bash

    # Using default GENERIC catalog configuration
    curl -X POST "http://localhost:8000/upload-sky-survey-batch" \\
      -F "files=@test_catalog_1.csv;type=text/csv" \\
      -F "files=@test_catalog_2.csv;type=text/csv"
    
    # Using GLEAM catalog configuration
    curl -X POST "http://localhost:8000/upload-sky-survey-batch" \\
      -F "catalog=GLEAM" \\
      -F "files=@gleam_survey.csv;type=text/csv"
    
    # Using RACS catalog configuration
    curl -X POST "http://localhost:8000/upload-sky-survey-batch" \\
      -F "catalog=RACS" \\
      -F "files=@racs_survey.csv;type=text/csv"

**Python Example**:

.. code-block:: python

    import requests

    url = "http://localhost:8000/upload-sky-survey-batch"
    
    # Option 1: Use predefined catalog
    files = [
        ("files", ("test_catalog_1.csv", open("test_catalog_1.csv", "rb"), "text/csv")),
        ("files", ("test_catalog_2.csv", open("test_catalog_2.csv", "rb"), "text/csv")),
    ]
    response = requests.post(url, files=files, data={"catalog": "GLEAM"})
    
    # Option 2: Use default (GENERIC) configuration
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

Catalog Configuration Selection
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The system provides four predefined catalog configurations:

**GENERIC** (default):
    - Generic GLEAM-format CSV support
    - Suitable for most standard sky survey formats
    - Automatically used if no catalog is specified

**GLEAM**:
    - Murchison Widefield Array (MWA) GLEAM catalog
    - Frequency range: 80-300 MHz
    - Supports 20 frequency bands
    - Source column: ``GLEAM``

**RACS**:
    - ASKAP RACS (Rapid ASKAP Continuum Survey)
    - Frequency range: 700-1800 MHz
    - Custom column mappings for RACS format
    - Source column: ``RACS``

**RCAL**:
    - Realtime calibration test data
    - GLEAM-format with 20 frequency bands
    - Frequency range: 80-300 MHz
    - Source column: ``GLEAM``

Selection Priority:
    1. **config** parameter (custom dict) - highest priority
    2. **catalog** parameter (predefined name)
    3. **GENERIC** - default if neither specified

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

**Missing Configuration** (400):

.. code-block:: json

    {
        "detail": "Unknown catalog 'INVALID'. Available: ['GLEAM', 'RACS', 'RCAL', 'GENERIC']"
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

3. **Error Handling**: Always check the response status code and handle errors appropriately in your application.

4. **Batch Size**: Consider breaking very large uploads into smaller batches for better manageability.

5. **Column Naming**: Use standard GLEAM-style column names (RAJ2000, DEJ2000, Fpwide, etc.) for automatic mapping, 
   or provide custom ``heading_alias`` mappings in your configuration.

6. **Schema Compatibility**: The system automatically maps CSV columns to the GlobalSkyModel schema. 
   All fields from ``ska_sdp_datamodels.global_sky_model.SkySource`` are supported.

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
- **Main API** (``main.py``): Provides HTTP endpoints, coordinates workflow, and manages background tasks
- **Ingest Module** (``ingest.py``): Processes CSV data, validates against schema, and imports into database

Key Features:

- **Asynchronous Processing**: Background tasks via FastAPI BackgroundTasks keep API responsive
- **Modern Lifecycle Management**: Uses FastAPI lifespan context manager (not deprecated on_event)
- **Schema-Level Validation**: Data validated after transformation to standardized format
- **Atomic Operations**: Either all files are ingested or none are
- **Clean Temporary Storage**: Temporary files are always cleaned up, even on errors
- **State Tracking**: Upload status persists across the upload lifecycle
- **Error Isolation**: Failures are captured and reported without affecting other uploads

Validation Pipeline:

1. File type validation (CSV format, MIME type)
2. CSV parsing and transformation to schema format
3. Source-by-source validation (required fields, ranges, types)
4. Batch database insertion
5. Cleanup and status update
