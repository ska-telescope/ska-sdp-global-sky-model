# pylint: disable=no-member, too-many-positional-arguments
"""
A simple fastAPI to ingest data into the global sky model database and to obtain a local sky
model from it.
"""

# pylint: disable=too-many-arguments, broad-exception-caught, not-callable
import json
import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import BackgroundTasks, Depends, FastAPI, File, HTTPException, Request, UploadFile
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from sqlalchemy import func, text
from sqlalchemy.orm import Session
from starlette.middleware.cors import CORSMiddleware

from ska_sdp_global_sky_model.api.app.crud import get_local_sky_model
from ska_sdp_global_sky_model.api.app.ingest import ingest_catalogue
from ska_sdp_global_sky_model.api.app.models import (
    GlobalSkyModelMetadata,
    SkyComponent,
    SkyComponentStaging,
)
from ska_sdp_global_sky_model.api.app.request_responder import start_thread
from ska_sdp_global_sky_model.api.app.upload_manager import UploadManager
from ska_sdp_global_sky_model.configuration.config import (
    engine,
    get_db,
    templates,
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
def get_point_components(request: Request, db: Session = Depends(get_db)):
    """Retrieve all point components"""
    logger.info("Retrieving all point components...")
    components = db.query(SkyComponent).all()
    logger.info("Retrieved all point sources for all %s components", str(len(components)))
    return templates.TemplateResponse(
        "table.html", {"request": request, "items": list(components)}
    )


@app.get("/local_sky_model", response_class=HTMLResponse)
async def get_local_sky_model_endpoint(
    request: Request,
    ra: float,
    dec: float,
    fov: float,
    version: str | None = None,
    db: Session = Depends(get_db),
):
    """
    Get the local sky model from a global sky model.

    Args:
        request (Request): HTTP request object.
        ra (float): Right ascension of the observation point in degrees.
        dec (float): Declination of the observation point in degrees.
        fov (float): Field of view of the telescope in degrees.
        version (str): Version of the global sky model. Optional.
        db (Session): Database session object.

    Returns:
        html: An HTML template response of the LSM in table format.
    """
    logger.info(
        "Requesting local sky model with the following parameters: ra:%s, \
dec:%s, fov:%s, version:%s",
        ra,
        dec,
        fov,
        version,
    )
    local_model = get_local_sky_model(db, ra, dec, fov, version)
    return templates.TemplateResponse(
        "table.html", {"request": request, "items": list(local_model)}
    )


def _run_ingestion_task(upload_id: str, catalogue_metadata: GlobalSkyModelMetadata):
    """
    Run ingestion task in background to staging table.

    This function runs in a separate thread/task to keep the API responsive
    while processing large file uploads. Data is ingested to staging table
    and requires manual commit to move to main table.

    Parameters
    ----------
    upload_id : str
        Upload identifier for tracking
    catalogue_metadata : GlobalSkyModelMetadata
        Catalogue metadata for ingestion
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
            # Pass content directly
            catalogue_content_files = {"ingest": {"file_location": [{"content": content}]}}
            # Set staging flag and upload_id for tracking
            catalogue_metadata.staging = True

            logger.info(
                "Ingesting file to staging: %s, catalogue_version=%s",
                filename,
                catalogue_metadata.version,
            )
            if not ingest_catalogue(db, catalogue_metadata, catalogue_content_files):
                raise RuntimeError(f"Ingest failed for {filename}")

        # Mark as completed
        upload_manager.mark_completed(upload_id)
        logger.info("Background ingestion to staging completed for upload %s", upload_id)

    except Exception as e:
        error_msg = str(e)
        logger.error("Background ingestion failed for upload %s: %s", upload_id, error_msg)
        logger.exception("Full traceback for upload %s:", upload_id)

        if db:
            try:
                db.rollback()
                db.query(SkyComponentStaging).filter(
                    SkyComponentStaging.upload_id == upload_id
                ).delete()
                db.commit()
                logger.info("Cleared staged records for failed upload %s", upload_id)
            except Exception as cleanup_error:
                db.rollback()
                logger.error(
                    "Failed to clear staged records for upload %s: %s",
                    upload_id,
                    cleanup_error,
                )

        upload_manager.mark_failed(upload_id, error_msg)

    finally:
        # Cleanup memory
        upload_manager.cleanup(upload_id)

        if db:
            db.close()


@app.post(
    "/upload-sky-survey-batch",
    summary="Upload sky survey CSV files with catalogue metadata",
    description="Upload catalogue metadata file and CSV files for staging. "
    "Ingestion runs asynchronously - use the status endpoint to monitor progress.",
)
async def upload_sky_survey_batch(
    background_tasks: BackgroundTasks,
    metadata_file: UploadFile = File(..., description="catalogue metadata JSON file"),
    csv_files: list[UploadFile] = File(..., description="One or more CSV files"),
    db: Session = Depends(get_db),
):
    """
    Upload catalogue metadata and CSV files for staging.

    Requires a metadata.json file containing catalogue information and version,
    plus one or more CSV files with component data.

    Parameters
    ----------
    background_tasks : BackgroundTasks
        FastAPI background task manager
    metadata_file : UploadFile
        JSON file with catalogue metadata (version, catalogue_name, description, etc.)
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

    try:
        metadata_file_contents = await metadata_file.read()
    except Exception as exc:
        raise HTTPException(
            status_code=400, detail=f"Failed to read metadata file {metadata_file.filename}: {exc}"
        ) from exc

    # Validate CSV structure
    try:
        metadata = json.loads(metadata_file_contents.decode("utf-8"))
    except UnicodeDecodeError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"File {metadata_file.filename} is not valid UTF-8 text. "
            f"CSV files must be text-based.",
        ) from exc
    except json.JSONDecodeError as exc:
        logger.error("Metadata file %s is not valid JSON: %s", metadata_file.filename, exc)
        raise HTTPException(
            status_code=400,
            detail=f"Metadata file {metadata_file.filename} is not valid JSON: {exc}",
        ) from exc

    catalogue_metadata = GlobalSkyModelMetadata(
        version=metadata.get("version"),
        catalogue_name=metadata.get("catalogue_name", "UPLOAD"),
        description=metadata.get("description", ""),
        upload_id="upload_id_placeholder",  # Will be set after creating upload status
        ref_freq=metadata.get("ref_freq"),
        epoch=metadata.get("epoch"),
        author=metadata.get("author"),
        reference=metadata.get("reference"),
        notes=metadata.get("notes"),
    )

    # Create upload tracking
    upload_status = upload_manager.create_upload(len(csv_files), catalogue_metadata)
    catalogue_metadata.upload_id = upload_status.upload_id

    try:
        logger.info(
            "Received upload with metadata: version=%s, catalogue_name=%s, \
            ref_freq=%s, epoch=%s, upload_id=%s",
            catalogue_metadata.version,
            catalogue_metadata.catalogue_name,
            catalogue_metadata.ref_freq,
            catalogue_metadata.epoch,
            catalogue_metadata.upload_id,
        )

        # Validate version format and check it's an increment
        existing_versions = [v[0] for v in db.query(GlobalSkyModelMetadata.version).all()]
        is_valid, error_msg = is_version_increment(catalogue_metadata.version, existing_versions)
        if not is_valid:
            raise HTTPException(status_code=400, detail=error_msg)

        # Check version doesn't already exist
        existing = (
            db.query(GlobalSkyModelMetadata)
            .filter(GlobalSkyModelMetadata.version == catalogue_metadata.version)
            .first()
        )
        if existing:
            raise HTTPException(
                status_code=400,
                detail=f"Version '{catalogue_metadata.version}' already exists. "
                f"Please use a higher version number.",
            )

        # Validate and save CSV files
        for file in csv_files:
            await upload_manager.save_csv_file(file, upload_status)

        # Schedule ingestion to run in background
        background_tasks.add_task(
            _run_ingestion_task, catalogue_metadata.upload_id, catalogue_metadata
        )

        logger.info(
            "Upload %s: metadata and %d CSV files saved, ingestion scheduled",
            catalogue_metadata.upload_id,
            len(csv_files),
        )

        return {
            "upload_id": catalogue_metadata.upload_id,
            "status": "uploading",
            "version": catalogue_metadata.version,
            "catalogue_name": catalogue_metadata.catalogue_name,
            "message": f"Uploaded {len(csv_files)} CSV file(s) with metadata. Ingestion running.",
        }

    except HTTPException as exc:
        upload_manager.mark_failed(catalogue_metadata.upload_id, str(exc.detail))
        upload_manager.cleanup(catalogue_metadata.upload_id)
        raise

    except Exception as e:
        error_msg = str(e)
        upload_manager.mark_failed(catalogue_metadata.upload_id, error_msg)
        upload_manager.cleanup(catalogue_metadata.upload_id)
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

    response = {
        "upload_id": upload_id,
        "total_records": count,
        "sample_range": f"{sample_start}-{sample_end}",
        "sample": [row.columns_to_dict() for row in sample],
    }
    # Add metadata details if available
    if status.metadata:
        response["metadata"] = status.metadata
    return response


@app.post("/commit-upload/{upload_id}")
def commit_upload(upload_id: str, db: Session = Depends(get_db)):
    """
    Commit staged data to main database with catalogue-level versioning.

    Creates a GlobalSkyModelMetadata record and copies all components from staging
    to the main table with the catalogue version.

    Parameters
    ----------
    upload_id : str
        Upload identifier
    db : Session
        Database session

    Returns
    -------
    dict
        Result of commit operation including version and catalogue info

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
            detail="No metadata found for this upload. Cannot commit without catalogue metadata.",
        )

    try:
        # Get all staged records
        staged_records = (
            db.query(SkyComponentStaging).filter(SkyComponentStaging.upload_id == upload_id).all()
        )

        if not staged_records:
            raise HTTPException(status_code=404, detail="No staged data found")

        metadata = status.metadata
        catalogue_version = metadata.version
        catalogue_name = metadata.catalogue_name

        db.add(status.metadata)

        # Copy from staging to main table with catalogue version
        for staged in staged_records:
            # Create main table record from staged data
            # Exclude 'id' and 'upload_id' from staging table fields
            record_data = {
                k: v for k, v in staged.columns_to_dict().items() if k not in ["id", "upload_id"]
            }
            # Set catalogue version for ALL components
            record_data["version"] = catalogue_version

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
            catalogue_version,
        )

        return {
            "status": "success",
            "message": f"Committed {len(staged_records)} \
                components from catalogue '{catalogue_name}'",
            "records_committed": len(staged_records),
            "version": catalogue_version,
            "catalogue_name": catalogue_name,
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


@app.get("/catalogue-metadata", summary="Query catalogue metadata")
def get_catalogue_metadata(
    catalogue_name: str | None = None,
    version: str | None = None,
    limit: int = 100,
    db: Session = Depends(get_db),
):
    """
    Query catalogue metadata records.

    Search by catalogue name, version, or list all catalogues.
    Results are ordered by upload date (newest first).

    Parameters
    ----------
    catalogue_name : str, optional
        Filter by catalogue name (case-insensitive partial match)
    version : str, optional
        Filter by exact version
    limit : int, default 100
        Maximum number of results to return
    db : Session
        Database session

    Returns
    -------
    dict
        List of catalogue metadata records
    """
    query = db.query(GlobalSkyModelMetadata)

    # Apply filters
    if catalogue_name:
        query = query.filter(GlobalSkyModelMetadata.catalogue_name.ilike(f"%{catalogue_name}%"))

    if version:
        query = query.filter(GlobalSkyModelMetadata.version == version)

    # Order by most recent first
    query = query.order_by(GlobalSkyModelMetadata.uploaded_at.desc())

    # Apply limit
    query = query.limit(limit)

    # Execute query
    results = query.all()

    return {
        "total": len(results),
        "catalogues": [catalogue.to_dict() for catalogue in results],
    }


@app.get("/catalogue-metadata/{catalogue_id}", summary="Get specific catalogue metadata")
def get_catalogue_metadata_by_id(
    catalogue_id: int,
    db: Session = Depends(get_db),
):
    """
    Get catalogue metadata by ID.

    Parameters
    ----------
    catalogue_id : int
        catalogue metadata ID
    db : Session
        Database session

    Returns
    -------
    dict
        catalogue metadata record

    Raises
    ------
    HTTPException
        If catalogue not found
    """
    catalogue = (
        db.query(GlobalSkyModelMetadata).filter(GlobalSkyModelMetadata.id == catalogue_id).first()
    )

    if not catalogue:
        raise HTTPException(status_code=404, detail=f"catalogue with ID {catalogue_id} not found")

    return catalogue.to_dict()
