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
- Automatic data validation at the schema level
- Version control with semantic versioning for updated components

Staging and Versioning Workflow
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Two-Stage Upload Process**:

All uploads use a staging workflow for safety and review:

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


Upload Sky Survey
-----------------

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


API Endpoints
~~~~~~~~~~~~~

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
