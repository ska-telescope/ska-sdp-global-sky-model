Uploading Sky Survey Data
=========================

The SKA Global Sky Model (GSM) provides both a browser interface and API endpoints for uploading multiple sky survey catalogue files in a single atomic batch operation into the GSM database.

Overview
--------

The batch upload feature allows you to:

- Upload multiple CSV files simultaneously via API or browser interface (all files are combined into a single sky model)
- Provide catalog metadata via JSON file (required - includes version, name, description, reference frequency, and epoch)
- Upload multiple CSV files simultaneously via API or browser interface (all files are combined into a single catalog version)
- Stage uploads for review before committing to the main database
- Track upload progress with a unique identifier
- Query upload status and errors
- Ensure atomic ingestion (all files succeed or none are ingested)
- Process uploads asynchronously in the background for optimal API responsiveness
- Automatic data validation at the schema level
- Catalog-level semantic versioning for tracking catalog releases

Catalog Metadata File
~~~~~~~~~~~~~~~~~~~~~

**Required for All Uploads**:

Every upload must include a ``metadata.json`` file containing catalog-level information that applies 
to all components in the batch. The metadata follows the ``GlobalSkyModelMetadata`` dataclass format 
from ska-sdp-datamodels.

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
    - ``catalogue_name``: Catalog identifier (e.g., "GLEAM", "RACS", "RCAL")
    - ``description``: Human-readable description of the catalog
    - ``ref_freq``: Reference frequency in Hz (float/integer)
    - ``epoch``: Epoch of observation (e.g., "J2000")

**Optional Fields**:
    - ``author``: Author or team name
    - ``reference``: DOI, URL, or citation
    - ``notes``: Additional information

**Version Requirements**:
    - Must follow semantic versioning: ``major.minor.patch`` (e.g., "1.0.0", "2.1.3")
    - Each version must be unique - cannot reuse existing version numbers
    - New version must be greater than all existing versions for that catalog
    - The same version applies to ALL components in the upload

Staging and Versioning Workflow
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Two-Stage Upload Process**:

All uploads use a staging workflow allowing new catalog versions to be successfully uploaded before a new version is released.
When committing staged data, the system sets the version for all components in the upload to the same value from the metadata file:

1. **Upload with Metadata**: Upload CSV files plus metadata.json to ``sky_component_staging`` table
2. **Review Upload Status**: Use ``/review-upload/{upload_id}`` to confirm the upload succeeded
3. **Commit or Reject** (manual action required): 
   - Commit: Move data to main table with catalog version from metadata
   - Reject: Discard all staged data for that upload

**Catalog-Level Versioning**:

Version tracking is now done at the catalog level, not per-component:

- **Single version per upload**: All components in a batch receive the same version from metadata.json
- **Semantic versioning**: Versions must follow ``major.minor.patch`` format (e.g., "1.0.0")
- **Version increments**: Each new upload must have a version greater than all previous versions
- **Catalog metadata table**: New ``catalog_metadata`` table tracks catalog versions, upload metadata, and timestamps
- **Query capabilities**: Use ``GET /catalog-metadata`` to list all catalog versions and metadata

This approach allows you to:
    - Track entire catalog releases with proper semantic versioning
    - Associate metadata (frequency, epoch, references) with each catalog version
    - Query catalogs by name and version
    - Maintain a complete history of catalog updates with timestamps and authorship

Files uploaded in the same session (same ``upload_id``) will form part of the same catalog version. 
Files uploaded in a new session (new ``upload_id``) will create a new catalog version with its own version number as per the metadata.json file. Uploading a new version requires incrementing the version number in the metadata file to ensure proper version tracking, duplicate version numbers are not allowed.

**Staging Table Schema**:

The ``sky_component_staging`` table mirrors the main ``sky_component`` table but includes:
    - ``upload_id``: Links all sources in a batch upload (same session)
    - Unique constraint: ``component_id + upload_id``

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

The uploaded CSV files must follow the standardized sky survey catalogue format compatible with the 
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
    - ``pol_ang``: Polarization angle in degrees (float)
    - ``rot_meas``: Faraday rotation measure in rad/m² (float)

CSV Format Examples
~~~~~~~~~~~~~~~~~~~~

**Standardized Format**:

The ``test_catalogue_1.csv`` and ``test_catalogue_2.csv`` files in the test data directory demonstrate 
the required standardized format:

.. code-block:: text

    component_id,ra,dec,i_pol,major_ax,minor_ax,pos_ang,spec_idx,log_spec_idx
    J025837+035057,44.656883,3.849425,0.835419,142.417,132.7302,3.451346,-0.419238,False
    J030420+022029,46.084633,2.341634,0.29086,137.107,134.2583,-0.666618,-1.074094,False

These test catalogues contain 100 components each and are used throughout the test suite as reference examples.

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

A browser interface is available at the ``/upload`` endpoint (e.g., ``<GSM_API_URL>/upload``). The interface provides:

- **Drag-and-drop file upload**: Simply drag CSV files onto the upload zone
- **Multiple file selection**: Upload multiple files that form a single catalogue
- **Real-time status monitoring**: Track upload progress with automatic polling
- **Upload confirmation**: Confirm successful upload before committing to database
- **Commit/Reject workflow**: Approve or discard staged uploads
- **Version information**: Visual indicators showing which catalogue version will be created

**Using the Browser Interface**:

1. Navigate to ``<GSM_API_URL>/upload`` in your web browser (replace ``<GSM_API_URL>`` with your deployment URL)
2. Drag and drop CSV files onto the upload zone (or click to browse)
3. Click "Upload Files" to begin the upload
4. Monitor the upload progress - status updates automatically
5. Confirm the upload completed successfully and review the count of staged records
6. Click "Commit to Database" to approve or "Reject and Discard" to cancel

The browser interface automatically handles:
    - File validation (CSV format only) - see `Data Validation`_ for details
    - Upload tracking with unique IDs
    - Status polling every 2 seconds
    - Error display if uploads fail


API Endpoints
~~~~~~~~~~~~~

**Endpoint**: ``POST /upload-sky-survey-batch``

Upload and ingest one or more sky survey CSV files along with catalog metadata to the staging table. 
All files in the batch are combined into a single sky model with a shared catalog version. All files 
are validated and uploaded to a staging area before ingestion begins. If any file fails validation 
or ingestion, the entire batch is rolled back.

**Parameters**:

.. list-table::
    :widths: 20, 50, 20, 10
    :header-rows: 1

    * - Parameter
      - Description
      - Data Type
      - Required
    * - ``metadata_file``
      - JSON file containing catalog metadata (version, catalogue_name, description, ref_freq, epoch)
      - File
      - Yes
    * - ``csv_files``
      - One or more CSV files containing standardized sky survey data
      - list[File]
      - Yes

**Response**:

.. code-block:: json

    {
        "upload_id": "550e8400-e29b-41d4-a716-446655440000",
        "status": "uploading",
        "version": "1.0.0",
        "catalogue_name": "GLEAM"
    }

**Note**: The endpoint returns immediately with status "uploading". Ingestion to staging table proceeds 
asynchronously in the background. Use the status endpoint to monitor completion, then review and commit.

**Example Usage**:

.. code-block:: bash

    # Upload CSV files with catalog metadata
    curl -X POST "http://localhost:8000/upload-sky-survey-batch" \\
      -F "metadata_file=@metadata.json;type=application/json" \\
      -F "csv_files=@test_catalogue_1.csv;type=text/csv" \\
      -F "csv_files=@test_catalogue_2.csv;type=text/csv"

**Python Example**:

.. code-block:: python

    import requests
    import time

    url = "<GSM_API_URL>/upload-sky-survey-batch"
    
    # Upload CSV files with catalog metadata
    files = [
        ("metadata_file", ("metadata.json", open("metadata.json", "rb"), "application/json")),
        ("csv_files", ("test_catalogue_1.csv", open("test_catalogue_1.csv", "rb"), "text/csv")),
        ("csv_files", ("test_catalogue_2.csv", open("test_catalogue_2.csv", "rb"), "text/csv")),
    ]
    response = requests.post(url, files=files)
    
    result = response.json()
    print(f"Upload ID: {result['upload_id']}")
    print(f"Catalog: {result['catalogue_name']} v{result['version']}")
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

    curl "<GSM_API_URL>/upload-sky-survey-status/550e8400-e29b-41d4-a716-446655440000"

**Python Example**:

.. code-block:: python

    import requests
    import time

    upload_id = "550e8400-e29b-41d4-a716-446655440000"
    url = f"<GSM_API_URL>/upload-sky-survey-status/{upload_id}"
    
    while True:
        response = requests.get(url)
        status = response.json()
        
        print(f"State: {status['state']}")
        print(f"Progress: {status['uploaded_csv_files']}/{status['total_csv_files']}")
        
        if status['state'] in ['completed', 'failed']:
            break
        
        time.sleep(2)
    
    if status['state'] == 'failed':
        print(f"Errors: {status['errors']}")

Review Staged Upload
~~~~~~~~~~~~~~~~~~~~

**Endpoint**: ``GET /review-upload/{upload_id}``

Review the status of the upload before committing to the main database. Returns total record count and the last 10 staged records to confirm all data loaded successfully.

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

    curl "<GSM_API_URL>/review-upload/550e8400-e29b-41d4-a716-446655440000"

**Python Example**:

.. code-block:: python

    import requests

    upload_id = "550e8400-e29b-41d4-a716-446655440000"
    url = f"<GSM_API_URL>/review-upload/{upload_id}"
    
    response = requests.get(url)
    review = response.json()
    
    print(f"Total records: {review['total_records']}")
    print(f"Sample data: {review['sample'][:3]}")  # First 3 records

Commit Staged Upload
~~~~~~~~~~~~~~~~~~~~

**Endpoint**: ``POST /commit-upload/{upload_id}``

Commit staged data to the main database with the catalog version from the metadata file. All components 
in the upload receive the same version. Creates a record in the ``catalog_metadata`` table with the 
upload information.

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
        "upload_id": "550e8400-e29b-41d4-a716-446655440000",
        "records_committed": 200,
        "version": "1.0.0",
        "catalogue_name": "GLEAM"
    }

**Example Usage**:

.. code-block:: bash

    curl -X POST "<GSM_API_URL>/commit-upload/550e8400-e29b-41d4-a716-446655440000"

**Python Example**:

.. code-block:: python

    import requests

    upload_id = "550e8400-e29b-41d4-a716-446655440000"
    url = f"<GSM_API_URL>/commit-upload/{upload_id}"
    
    response = requests.post(url)
    result = response.json()
    
    print(f"Committed {result['records_committed']} records")
    print(f"Catalog: {result['catalogue_name']} v{result['version']}")

Query Catalog Metadata
~~~~~~~~~~~~~~~~~~~~~~~

**Endpoint**: ``GET /catalog-metadata``

Query catalog metadata records by catalog name, version, or list all catalogs. Returns catalog 
version information including upload dates, reference frequencies, epochs, and authorship details.

**Parameters**:

.. list-table::
    :widths: 20, 50, 20, 10
    :header-rows: 1

    * - Parameter
      - Description
      - Data Type
      - Required
    * - ``catalogue_name``
      - Filter by catalog name (case-insensitive partial match)
      - string
      - No
    * - ``version``
      - Filter by exact version
      - string
      - No
    * - ``limit``
      - Maximum number of results to return (default: 100)
      - integer
      - No

**Response**:

.. code-block:: json

    {
        "total": 2,
        "catalogs": [
            {
                "id": 1,
                "version": "2.0.0",
                "catalogue_name": "GLEAM",
                "description": "GaLactic and Extragalactic All-sky MWA Survey",
                "upload_id": "550e8400-e29b-41d4-a716-446655440000",
                "uploaded_at": "2026-02-13T10:30:00",
                "ref_freq": 170000000.0,
                "epoch": "J2000",
                "author": "GLEAM Team",
                "reference": "https://doi.org/10.1093/mnras/stw2337",
                "notes": "170 MHz continuum survey"
            },
            {
                "id": 2,
                "version": "1.0.0",
                "catalogue_name": "GLEAM",
                "description": "GaLactic and Extragalactic All-sky MWA Survey - Initial Release",
                "upload_id": "660e8400-e29b-41d4-a716-446655440001",
                "uploaded_at": "2026-01-15T09:15:00",
                "ref_freq": 170000000.0,
                "epoch": "J2000",
                "author": "GLEAM Team",
                "reference": "https://doi.org/10.1093/mnras/stw2337",
                "notes": null
            }
        ]
    }

**Example Usage**:

.. code-block:: bash

    # List all catalogs
    curl "http://localhost:8000/catalog-metadata"

    # Get specific catalog versions
    curl "http://localhost:8000/catalog-metadata?catalogue_name=GLEAM"

    # Get specific version
    curl "http://localhost:8000/catalog-metadata?version=1.0.0"

    # Combined search with limit
    curl "http://localhost:8000/catalog-metadata?catalogue_name=GLEAM&limit=10"

**Python Example**:

.. code-block:: python

    import requests

    # Query all GLEAM catalog versions
    url = "http://localhost:8000/catalog-metadata"
    params = {"catalogue_name": "GLEAM"}
    
    response = requests.get(url, params=params)
    result = response.json()
    
    print(f"Found {result['total']} catalog versions:")
    for catalog in result['catalogs']:
        print(f"  {catalog['catalogue_name']} v{catalog['version']}")
        print(f"    Uploaded: {catalog['uploaded_at']}")
        print(f"    Reference frequency: {catalog['ref_freq']/1e6:.1f} MHz")
        print(f"    Epoch: {catalog['epoch']}")

Reject Staged Upload
~~~~~~~~~~~~~~~~~~~~~**Python Example**:

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

    curl -X DELETE "<GSM_API_URL>/reject-upload/550e8400-e29b-41d4-a716-446655440000"

**Python Example**:

.. code-block:: python

    import requests

    upload_id = "550e8400-e29b-41d4-a716-446655440000"
    url = f"<GSM_API_URL>/reject-upload/{upload_id}"
    
    response = requests.delete(url)
    result = response.json()
    
    print(f"Rejected and deleted {result['records_deleted']} records")
    print(f"Message: {result['message']}")
