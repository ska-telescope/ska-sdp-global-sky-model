# pylint: disable=no-member, too-many-positional-arguments
"""
A simple fastAPI to ingest data into the global sky model database and to obtain a local sky
model from it.
"""

# pylint: disable=too-many-arguments, broad-exception-caught, not-callable
import copy
import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import BackgroundTasks, Depends, FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse, ORJSONResponse
from sqlalchemy import func, text
from sqlalchemy.orm import Session
from starlette.middleware.cors import CORSMiddleware

from ska_sdp_global_sky_model.api.app.crud import get_local_sky_model
from ska_sdp_global_sky_model.api.app.ingest import ingest_catalog
from ska_sdp_global_sky_model.api.app.models import (
    CatalogMetadata,
    SkyComponent,
    SkyComponentStaging,
)
from ska_sdp_global_sky_model.api.app.request_responder import start_thread
from ska_sdp_global_sky_model.api.app.upload_manager import UploadManager
from ska_sdp_global_sky_model.configuration.config import (
    engine,
    get_db,
)
from ska_sdp_global_sky_model.utilities.version_utils import is_version_increment

logger = logging.getLogger(__name__)


def wait_for_db():
    """Await DB connection."""
    while True:
        try:
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            logger.info("Database is up and running!")
            break
        except Exception as e:
            logger.info("Database connection failed: %s", e)
            time.sleep(5)  # Wait before retrying


@asynccontextmanager
async def lifespan(fast_api_app: FastAPI):  # pylint: disable=unused-argument
    """
    Lifespan context manager for FastAPI application startup and shutdown.
    """
    # Startup
    logger.info("Starting application...")
    wait_for_db()
    start_thread()
    logger.info("Application startup complete")

    yield

    # Shutdown
    logger.info("Shutting down application...")


app = FastAPI(lifespan=lifespan)
app.add_middleware(GZipMiddleware, minimum_size=1000)

origins = []

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize upload manager
upload_manager = UploadManager()


def _get_db_session():
    """Get a fresh database session for background tasks."""
    db = next(get_db())
    try:
        return db
    finally:
        pass  # Don't close here, will be closed after use


@app.get("/ping", summary="Ping the API")
def ping():
    """Returns {"ping": "live"} when called"""
    logger.debug("Ping: alive")
    return {"ping": "live"}


@app.get("/upload", summary="Browser upload interface")
def upload_interface():
    """Serve the HTML upload interface"""
    upload_page = Path(__file__).parent / "static" / "upload.html"
    if upload_page.exists():
        return FileResponse(upload_page)
    return {"message": "Upload interface not available. Use API endpoints directly."}


@app.get("/components", summary="See all the point components")
def get_point_components(db: Session = Depends(get_db)):
    """Retrieve all point components"""
    logger.info("Retrieving all point components...")
    components = db.query(SkyComponent).all()
    logger.info("Retrieved all point components: %s components", str(len(components)))
    component_list = []
    for component in components:
        component_list.append([component.component_id, component.ra, component.dec])
    return component_list


@app.get("/local_sky_model", response_class=ORJSONResponse)
async def get_local_sky_model_endpoint(
    ra: str,
    dec: str,
    flux_wide: float,
    telescope: str,
    fov: float,
    db: Session = Depends(get_db),
):
    """
    Get the local sky model from a global sky model.

    Args:
        ra (float): Right ascension of the observation point in degrees.
        dec (float): Declination of the observation point in degrees.
        flux_wide (float): Wide-field flux of the observation in Jy.
        telescope (str): Name of the telescope being used for the observation.
        fov (float): Field of view of the telescope in arcminutes.

    Returns:
        dict: A dictionary containing the local sky model information.

        The dictionary includes the following keys:
            - ra: The right ascension provided as input.
            - dec: The declination provided as input.
            - flux_wide: The wide-field flux provided as input.
            - telescope: The telescope name provided as input.
            - fov: The field of view provided as input.
            - local_data: ......
    """
    logger.info(
        "Requesting local sky model with the following parameters: ra:%s, \
dec:%s, flux_wide:%s, telescope:%s, fov:%s",
        ra,
        dec,
        flux_wide,
        telescope,
        fov,
    )
    local_model = get_local_sky_model(db, ra.split(";"), dec.split(";"), flux_wide, telescope, fov)
    return ORJSONResponse(local_model)


def _run_ingestion_task(upload_id: str, survey_metadata: dict):
    """
    Run ingestion task in background to staging table.

    This function runs in a separate thread/task to keep the API responsive
    while processing large file uploads. Data is ingested to staging table
    and requires manual commit to move to main table.

    Parameters
    ----------
    upload_id : str
        Upload identifier for tracking
    survey_metadata : dict
        Catalog metadata for ingestion
    """
    db = None
    try:
        # Get fresh database session
        db = _get_db_session()

        # Get files from memory
        files_data = upload_manager.get_files(upload_id)

        # Ingest all files from memory to staging table
        for filename, content in files_data:
            # Deep copy to avoid modifying shared metadata
            file_metadata = copy.deepcopy(survey_metadata)
            # Pass content directly
            file_metadata["ingest"]["file_location"][0]["content"] = content
            # Set staging flag and upload_id for tracking
            file_metadata["staging"] = True
            file_metadata["upload_id"] = upload_id

            logger.info("Ingesting file to staging: %s", filename)
            if not ingest_catalog(db, file_metadata):
                raise RuntimeError(f"Ingest failed for {filename}")

        # Mark as completed
        upload_manager.mark_completed(upload_id)
        logger.info("Background ingestion to staging completed for upload %s", upload_id)

    except Exception as e:
        error_msg = str(e)
        logger.error("Background ingestion failed for upload %s: %s", upload_id, error_msg)
        logger.exception("Full traceback for upload %s:", upload_id)
        upload_manager.mark_failed(upload_id, error_msg)

    finally:
        # Cleanup memory
        upload_manager.cleanup(upload_id)

        if db:
            db.close()


@app.post(
    "/upload-sky-survey-batch",
    summary="Upload sky survey CSV files with catalog metadata",
    description="Upload catalog metadata file and CSV files for staging. "
    "Ingestion runs asynchronously - use the status endpoint to monitor progress.",
)
async def upload_sky_survey_batch(
    background_tasks: BackgroundTasks,
    metadata_file: UploadFile = File(..., description="Catalog metadata JSON file"),
    csv_files: list[UploadFile] = File(..., description="One or more CSV files"),
    db: Session = Depends(get_db),
):
    """
    Upload catalog metadata and CSV files for staging.

    Requires a metadata.json file containing catalog information and version,
    plus one or more CSV files with component data.

    Parameters
    ----------
    background_tasks : BackgroundTasks
        FastAPI background task manager
    metadata_file : UploadFile
        JSON file with catalog metadata (version, catalog_name, description, etc.)
    csv_files : list[UploadFile]
        One or more CSV files containing component data
    db : Session
        Database session

    Raises
    ------
    HTTPException
        If validation fails or version is invalid

    Returns
    -------
    dict
        Upload identifier and status
    """
    if not csv_files:
        raise HTTPException(status_code=400, detail="No CSV files provided")

    # Create upload tracking
    upload_status = upload_manager.create_upload(len(csv_files))
    upload_id = upload_status.upload_id

    try:
        # 1. Parse and validate metadata file
        await upload_manager.save_metadata_file(metadata_file, upload_status)
        metadata = upload_status.metadata

        # 2. Validate version format and check it's an increment
        existing_versions = [v[0] for v in db.query(CatalogMetadata.version).all()]
        is_valid, error_msg = is_version_increment(metadata["version"], existing_versions)
        if not is_valid:
            raise HTTPException(status_code=400, detail=error_msg)

        # 3. Check version doesn't already exist
        existing = (
            db.query(CatalogMetadata)
            .filter(CatalogMetadata.version == metadata["version"])
            .first()
        )
        if existing:
            raise HTTPException(
                status_code=400,
                detail=f"Version '{metadata['version']}' already exists. "
                f"Please use a higher version number.",
            )

        # 4. Validate and save CSV files
        for file in csv_files:
            await upload_manager.save_csv_file(file, upload_status)

        # 5. Prepare metadata for ingestion
        survey_metadata = {
            "name": metadata.get("catalog_name", "Upload"),
            "catalog_name": metadata.get("catalog_name", "UPLOAD"),
            "version": metadata["version"],
            "description": metadata.get("description", ""),
            "ingest": {
                "file_location": [
                    {
                        "content": None,  # Will be filled per-file
                    }
                ],
            },
        }

        # 6. Schedule ingestion to run in background
        background_tasks.add_task(_run_ingestion_task, upload_id, survey_metadata)

        logger.info(
            "Upload %s: metadata and %d CSV files saved, ingestion scheduled",
            upload_id,
            len(csv_files),
        )

        return {
            "upload_id": upload_id,
            "status": "uploading",
            "version": metadata["version"],
            "catalog_name": metadata["catalog_name"],
            "message": f"Uploaded {len(csv_files)} CSV file(s) with metadata. Ingestion running.",
        }

    except HTTPException:
        upload_manager.mark_failed(upload_id, "HTTP exception during upload")
        upload_manager.cleanup(upload_id)
        raise

    except Exception as e:
        error_msg = str(e)
        upload_manager.mark_failed(upload_id, error_msg)
        upload_manager.cleanup(upload_id)
        raise HTTPException(
            status_code=500,
            detail=f"Sky survey upload failed: {error_msg}",
        ) from e


@app.get("/upload-sky-survey-status/{upload_id}")
def upload_sky_survey_status(upload_id: str):
    """
    Retrieve the current status of a sky survey upload.

    Parameters
    ----------
    upload_id : str
        Unique identifier returned when the upload was initiated.

    Raises
    ------
    HTTPException
        If the upload ID does not exist.

    Returns
    -------
    dict
        Upload progress, completion state, and error information.
    """
    status = upload_manager.get_status(upload_id)
    return status.to_dict()


@app.get("/review-upload/{upload_id}")
def review_upload(upload_id: str, db: Session = Depends(get_db)):
    """
    Review staged data before committing to main database.

    Parameters
    ----------
    upload_id : str
        Upload identifier
    db : Session
        Database session

    Returns
    -------
    dict
        Sample of staged data and statistics
    """
    status = upload_manager.get_status(upload_id)

    if status.state != "completed":
        raise HTTPException(
            status_code=400, detail=f"Upload not ready for review. Current state: {status.state}"
        )

    # Get count and sample of staged data
    count = (
        db.query(func.count(SkyComponentStaging.id))
        .filter(SkyComponentStaging.upload_id == upload_id)
        .scalar()
    )

    # Get last 10 rows as sample to confirm all data loaded
    sample = (
        db.query(SkyComponentStaging)
        .filter(SkyComponentStaging.upload_id == upload_id)
        .order_by(SkyComponentStaging.id.desc())
        .limit(10)
        .all()
    )

    # Reverse to show in ascending order and calculate positions
    sample.reverse()

    # Calculate the starting position
    sample_start = max(1, count - len(sample) + 1)
    sample_end = count

    return {
        "upload_id": upload_id,
        "total_records": count,
        "sample_range": f"{sample_start}-{sample_end}",
        "sample": [row.columns_to_dict() for row in sample],
    }


@app.post("/commit-upload/{upload_id}")
def commit_upload(upload_id: str, db: Session = Depends(get_db)):
    """
    Commit staged data to main database with catalog-level versioning.

    Creates a CatalogMetadata record and copies all components from staging
    to the main table with the catalog version.

    Parameters
    ----------
    upload_id : str
        Upload identifier
    db : Session
        Database session

    Returns
    -------
    dict
        Result of commit operation including version and catalog info

    Raises
    ------
    HTTPException
        If upload not ready, no metadata, or commit fails
    """
    status = upload_manager.get_status(upload_id)

    if status.state != "completed":
        raise HTTPException(
            status_code=400, detail=f"Upload not ready for commit. Current state: {status.state}"
        )

    if not status.metadata:
        raise HTTPException(
            status_code=400,
            detail="No metadata found for this upload. Cannot commit without catalog metadata.",
        )

    try:
        # Get all staged records
        staged_records = (
            db.query(SkyComponentStaging).filter(SkyComponentStaging.upload_id == upload_id).all()
        )

        if not staged_records:
            raise HTTPException(status_code=404, detail="No staged data found")

        metadata = status.metadata
        catalog_version = metadata["version"]
        catalog_name = metadata["catalog_name"]

        # Create CatalogMetadata record
        catalog_metadata = CatalogMetadata(
            version=catalog_version,
            catalog_name=catalog_name,
            description=metadata.get("description", ""),
            upload_id=upload_id,
            ref_freq=float(metadata["ref_freq"]),
            epoch=metadata["epoch"],
            author=metadata.get("author"),
            reference=metadata.get("reference"),
            notes=metadata.get("notes"),
        )
        db.add(catalog_metadata)

        # Copy from staging to main table with catalog version
        for staged in staged_records:
            # Create main table record from staged data
            # Exclude 'id' and 'upload_id' from staging table fields
            record_data = {
                k: v for k, v in staged.columns_to_dict().items() if k not in ["id", "upload_id"]
            }
            # Set catalog version for ALL components
            record_data["version"] = catalog_version

            main_record = SkyComponent(**record_data)
            db.add(main_record)

        # Delete from staging
        db.query(SkyComponentStaging).filter(SkyComponentStaging.upload_id == upload_id).delete()

        db.commit()

        # Cleanup temp files
        upload_manager.cleanup(upload_id)

        logger.info(
            "Successfully committed upload %s: %d components with version %s",
            upload_id,
            len(staged_records),
            catalog_version,
        )

        return {
            "status": "success",
            "message": f"Committed {len(staged_records)} components from catalog '{catalog_name}'",
            "records_committed": len(staged_records),
            "version": catalog_version,
            "catalog_name": catalog_name,
        }

    except Exception as e:
        db.rollback()
        logger.error("Failed to commit upload %s: %s", upload_id, e)
        raise HTTPException(status_code=500, detail=f"Commit failed: {str(e)}") from e


@app.delete("/reject-upload/{upload_id}")
def reject_upload(upload_id: str, db: Session = Depends(get_db)):
    """
    Reject and discard staged data.

    Parameters
    ----------
    upload_id : str
        Upload identifier
    db : Session
        Database session

    Returns
    -------
    dict
        Result of reject operation
    """
    status = upload_manager.get_status(upload_id)

    if status.state != "completed":
        raise HTTPException(
            status_code=400,
            detail=f"Upload not ready for rejection. Current state: {status.state}",
        )

    try:
        # Count records to be deleted
        count = (
            db.query(SkyComponentStaging)
            .filter(SkyComponentStaging.upload_id == upload_id)
            .count()
        )

        # Delete staged data
        db.query(SkyComponentStaging).filter(SkyComponentStaging.upload_id == upload_id).delete()

        db.commit()

        # Mark upload as failed
        upload_manager.mark_failed(upload_id, "Rejected by user")

        # Cleanup temp files
        upload_manager.cleanup(upload_id)

        logger.info("Rejected and deleted %d staged records for upload %s", count, upload_id)

        return {
            "status": "success",
            "message": f"Rejected and deleted {count} staged records",
            "records_deleted": count,
        }

    except Exception as e:
        db.rollback()
        logger.error("Failed to reject upload %s: %s", upload_id, e)
        raise HTTPException(status_code=500, detail=f"Reject failed: {str(e)}") from e


@app.get("/catalog-metadata", summary="Query catalog metadata")
def get_catalog_metadata(
    catalog_name: str | None = None,
    version: str | None = None,
    limit: int = 100,
    db: Session = Depends(get_db),
):
    """
    Query catalog metadata records.

    Search by catalog name, version, or list all catalogs.
    Results are ordered by upload date (newest first).

    Parameters
    ----------
    catalog_name : str, optional
        Filter by catalog name (case-insensitive partial match)
    version : str, optional
        Filter by exact version
    limit : int, default 100
        Maximum number of results to return
    db : Session
        Database session

    Returns
    -------
    dict
        List of catalog metadata records
    """
    query = db.query(CatalogMetadata)

    # Apply filters
    if catalog_name:
        query = query.filter(CatalogMetadata.catalog_name.ilike(f"%{catalog_name}%"))

    if version:
        query = query.filter(CatalogMetadata.version == version)

    # Order by most recent first
    query = query.order_by(CatalogMetadata.uploaded_at.desc())

    # Apply limit
    query = query.limit(limit)

    # Execute query
    results = query.all()

    return {
        "total": len(results),
        "catalogs": [catalog.to_dict() for catalog in results],
    }


@app.get("/catalog-metadata/{catalog_id}", summary="Get specific catalog metadata")
def get_catalog_metadata_by_id(
    catalog_id: int,
    db: Session = Depends(get_db),
):
    """
    Get catalog metadata by ID.

    Parameters
    ----------
    catalog_id : int
        Catalog metadata ID
    db : Session
        Database session

    Returns
    -------
    dict
        Catalog metadata record

    Raises
    ------
    HTTPException
        If catalog not found
    """
    catalog = db.query(CatalogMetadata).filter(CatalogMetadata.id == catalog_id).first()

    if not catalog:
        raise HTTPException(status_code=404, detail=f"Catalog with ID {catalog_id} not found")

    return catalog.to_dict()
