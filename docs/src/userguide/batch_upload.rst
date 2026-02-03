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
        "status": "completed"
    }

**Example Usage**:

.. code-block:: bash

    # Using default GENERIC catalog configuration
    curl -X POST "http://localhost:8000/upload-sky-survey-batch" \\
      -F "files=@survey1.csv;type=text/csv" \\
      -F "files=@survey2.csv;type=text/csv"
    
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
        ("files", ("survey1.csv", open("survey1.csv", "rb"), "text/csv")),
        ("files", ("survey2.csv", open("survey2.csv", "rb"), "text/csv")),
    ]
    response = requests.post(url, files=files, data={"catalog": "GLEAM"})
    
    # Option 2: Use default (GENERIC) configuration
    response = requests.post(url, files=files)
    
    result = response.json()
    print(f"Upload ID: {result['upload_id']}")
    print(f"Status: {result['status']}")

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

GLEAM Format Example
~~~~~~~~~~~~~~~~~~~~

The default configuration supports GLEAM-style catalogs:

.. code-block:: text

    recno,GLEAM,RAJ2000,DEJ2000,Fpwide,Fintwide,awide,bwide,pawide,alpha
    1,J000001-350001,0.004,-35.0,0.25,0.26,170,140,-6.2,-0.527
    2,J000002-350002,0.008,-35.1,0.23,0.24,168,138,-6.0,-0.534

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
        "detail": "Sky survey upload failed: Ingest failed for survey1.csv"
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

- **Upload Manager** (``upload_manager.py``): Handles file storage, validation, and state tracking
- **Main API** (``main.py``): Provides HTTP endpoints and coordinates the upload workflow
- **Ingest Module** (``ingest.py``): Processes and imports catalog data into the database

This architecture ensures:

- **Atomic Operations**: Either all files are ingested or none are
- **Clean Temporary Storage**: Temporary files are always cleaned up, even on errors
- **State Tracking**: Upload status persists across the upload lifecycle
- **Error Isolation**: Failures are captured and reported without affecting other uploads
