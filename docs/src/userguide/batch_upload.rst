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
- Automatic data validation at the schema level

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

After CSV files are loaded, each source undergoes validation:

**Required Fields**:
    - ``component_id``: Source identifier (string)
    - ``ra``: Right ascension (J2000) in degrees (float, -360 to 360)
    - ``dec``: Declination (J2000) in degrees (float, -90 to 90)
    - ``i_pol``: I polarization flux at reference frequency (float, must be positive, in Janskys)

**Validation Process**:
    - All sources are validated before any data is ingested
    - Validation errors are collected and logged for all sources
    - If ANY validation errors occur, NO data is ingested (all-or-nothing)
    - Only if ALL sources pass validation will ingestion proceed

API Endpoints
-------------

Upload Sky Survey Batch
~~~~~~~~~~~~~~~~~~~~~~~~

**Endpoint**: ``POST /upload-sky-survey-batch``

Upload and ingest one or more sky survey CSV files atomically. All files are validated and uploaded to a staging area before ingestion begins. If any file fails validation or ingestion, the entire batch is rolled back.

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

**Note**: The endpoint returns immediately with status "uploading". Ingestion proceeds asynchronously 
in the background. Use the status endpoint to monitor completion.

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

CSV File Format
---------------

The uploaded CSV files must follow the standardized sky survey catalog format compatible with the 
``ska_sdp_datamodels`` package. CSV files should use the standardized column names.

Required Columns
~~~~~~~~~~~~~~~~

Your CSV must include these standardized columns:

- **component_id**: Unique source identifier (string)
- **ra**: Right ascension (J2000) in degrees (float)
- **dec**: Declination (J2000) in degrees (float)
- **i_pol**: I polarization flux at reference frequency in Janskys (float, must be positive)

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

These test catalogs contain 100 sources each and are used throughout the test suite as reference examples.

**Minimal Format**:

At minimum, you need the four required columns:

.. code-block:: text

    component_id,ra,dec,i_pol
    J000001-350001,0.004,-35.0,0.25
    J000002-350002,0.008,-35.1,0.23

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

1. **File Validation**: Ensure all CSV files are properly formatted with standardized column names before upload 
   to avoid batch failures. Required columns are component_id, ra, dec, and i_pol.

2. **Progress Monitoring**: Use the status endpoint to monitor long-running uploads, especially for large batches.

3. **Error Handling**: Always check the response status code and handle errors appropriately in your application.

4. **Batch Size**: Consider breaking very large uploads into smaller batches for better manageability.
