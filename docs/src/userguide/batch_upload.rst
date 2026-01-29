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
    * - ``config``
      - Optional catalog configuration. If not provided, SKY_SURVEY config is used
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

    curl -X POST "http://localhost:8000/upload-sky-survey-batch" \\
      -F "files=@survey1.csv" \\
      -F "files=@survey2.csv" \\
      -F "files=@survey3.csv"

**Python Example**:

.. code-block:: python

    import requests

    url = "http://localhost:8000/upload-sky-survey-batch"
    
    files = [
        ("files", ("survey1.csv", open("survey1.csv", "rb"), "text/csv")),
        ("files", ("survey2.csv", open("survey2.csv", "rb"), "text/csv")),
    ]
    
    response = requests.post(url, files=files)
    result = response.json()
    
    print(f"Upload ID: {result['upload_id']}")
    print(f"Status: {result['status']}")

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

The uploaded CSV files should follow the standard sky survey catalog format with at minimum the following required columns:

- ``recno``: Record number (unique identifier)
- ``GLEAM`` or similar source name column
- ``RAJ2000``: Right ascension (J2000) in degrees
- ``DECJ2000``: Declination (J2000) in degrees

Additional columns for flux measurements, errors, and other source properties are supported depending on your catalog configuration.

Example CSV structure:

.. code-block:: text

    recno,GLEAM,RAJ2000,DECJ2000,Fpwide,e_Fpwide
    1,J000001-350001,0.004,-35.0,0.25,0.01
    2,J000002-350002,0.008,-35.1,0.23,0.01

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
        "detail": "SKY_SURVEY configuration not available. Please provide a config parameter."
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

2. **Progress Monitoring**: Use the status endpoint to monitor long-running uploads, especially for large batches.

3. **Error Handling**: Always check the response status code and handle errors appropriately in your application.

4. **Batch Size**: Consider breaking very large uploads into smaller batches for better manageability.

5. **Configuration**: Provide a custom configuration if your catalog format differs from the default SKY_SURVEY format.

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
