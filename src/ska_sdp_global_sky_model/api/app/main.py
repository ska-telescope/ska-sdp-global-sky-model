# pylint: disable=no-member, too-many-positional-arguments
"""
A simple fastAPI to obtain a local sky model from a global sky model.
"""

# pylint: disable=too-many-arguments, broad-exception-caught
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
from ska_sdp_global_sky_model.api.app.ingest import get_full_catalog
from ska_sdp_global_sky_model.api.app.models import SkyComponent, SkyComponentStaging
from ska_sdp_global_sky_model.api.app.request_responder import start_thread
from ska_sdp_global_sky_model.api.app.upload_manager import UploadManager
from ska_sdp_global_sky_model.configuration.config import STANDARD_CATALOG_CONFIG, engine, get_db

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

    Replaces deprecated @app.on_event("startup") and @app.on_event("shutdown").
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


@app.get("/", summary="Browser upload interface")
def root():
    """Serve the HTML upload interface"""
    upload_page = Path(__file__).parent / "static" / "upload.html"
    if upload_page.exists():
        return FileResponse(upload_page)
    return {"message": "Upload interface not available. Use API endpoints directly."}


def ingest(db: Session, catalog_config: dict):
    """Ingest catalog"""
    try:
        if get_full_catalog(db, catalog_config):
            return True
        logger.error("Error ingesting the catalogue")
        return False
    except Exception as e:  # pylint: disable=broad-exception-caught
        raise e


@app.get("/sources", summary="See all the point sources")
def get_point_sources(db: Session = Depends(get_db)):
    """Retrieve all point sources"""
    logger.info("Retrieving all point sources...")
    components = db.query(SkyComponent).all()
    logger.info("Retrieved all point sources for all %s components", str(len(components)))
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


def _run_ingestion_task(upload_id: str, survey_config: dict):
    """
    Run ingestion task in background to staging table.

    This function runs in a separate thread/task to keep the API responsive
    while processing large file uploads. Data is ingested to staging table
    and requires manual commit to move to main table.

    Parameters
    ----------
    upload_id : str
        Upload identifier for tracking
    survey_config : dict
        Catalog configuration for ingestion
    """
    db = None
    try:
        # Get fresh database session
        db = _get_db_session()

        # Ingest all files to staging table
        file_paths = upload_manager.list_files(upload_id)
        for file_path in file_paths:
            # Deep copy to avoid modifying shared config
            file_config = copy.deepcopy(survey_config)
            file_config["ingest"]["file_location"][0]["key"] = str(file_path)
            file_config["staging"] = True  # Flag to ingest to staging
            file_config["upload_id"] = upload_id  # Track upload batch

            if not get_full_catalog(db, file_config):
                raise RuntimeError(f"Ingest failed for {file_path.name}")

        # Mark as completed ingestion (still in UPLOADING, awaiting commit)
        upload_manager.mark_completed(upload_id)
        logger.info("Background ingestion to staging completed for upload %s", upload_id)

    except Exception as e:
        error_msg = str(e)
        logger.error("Background ingestion failed for upload %s: %s", upload_id, error_msg)
        logger.exception("Full traceback for upload %s:", upload_id)
        upload_manager.mark_failed(upload_id, error_msg)

    finally:
        # Cleanup temporary files
        upload_manager.cleanup(upload_id)
        if db:
            db.close()


@app.post(
    "/upload-sky-survey-batch",
    summary="Upload sky survey CSV files in a batch",
    description="All sky survey CSV files must upload successfully or none are ingested. "
    "Ingestion runs asynchronously - use the status endpoint to monitor progress.",
)
async def upload_sky_survey_batch(
    background_tasks: BackgroundTasks,
    files: list[UploadFile] = File(...),
    db: Session = Depends(get_db),
):
    """
    Upload and ingest one or more sky survey CSV files atomically.

    All files are first validated and written to a temporary staging directory.
    Ingestion then runs in the background, allowing the API to remain responsive.
    Use the status endpoint to monitor progress.

    Parameters
    ----------
    background_tasks : BackgroundTasks
        FastAPI background task manager
    files : list[UploadFile]
        One or more CSV files containing standardized sky survey data.
        Expected format: component_id,ra,dec,i_pol,major_ax,minor_ax,pos_ang,spec_idx,log_spec_idx
    db : Session
        Database session

    Raises
    ------
    HTTPException
        If validation or initial upload fails

    Returns
    -------
    dict
        Upload identifier for tracking status. Check /upload-sky-survey-status/{upload_id}
        for completion status.
    """
    if not files:
        raise HTTPException(status_code=400, detail="No files provided")

    # Validate database connection
    try:
        db.execute(text("SELECT 1"))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Unable to access database") from e

    # Use standard catalog configuration
    survey_config = copy.deepcopy(STANDARD_CATALOG_CONFIG)

    # Create upload tracking
    upload_status = upload_manager.create_upload(len(files))
    upload_id = upload_status.upload_id

    try:
        # Validate and save all files first (synchronous part)
        for file in files:
            upload_manager.validate_file(file)
            await upload_manager.save_file(file, upload_status)

        # Schedule ingestion to run in background
        background_tasks.add_task(_run_ingestion_task, upload_id, survey_config)

        logger.info("Upload %s: files saved, ingestion scheduled in background", upload_id)
        logger.info("===== Background task SCHEDULED for upload %s =====", upload_id)

        return {
            "upload_id": upload_id,
            "status": "uploading",
            "message": "Files uploaded successfully. Ingestion running in background.",
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

    # Get first 10 rows as sample
    sample = (
        db.query(SkyComponentStaging)
        .filter(SkyComponentStaging.upload_id == upload_id)
        .limit(10)
        .all()
    )

    return {
        "upload_id": upload_id,
        "total_records": count,
        "sample": [row.columns_to_dict() for row in sample],
    }


@app.post("/commit-upload/{upload_id}")
def commit_upload(upload_id: str, db: Session = Depends(get_db)):
    """
    Commit staged data to main database.

    Parameters
    ----------
    upload_id : str
        Upload identifier
    db : Session
        Database session

    Returns
    -------
    dict
        Result of commit operation
    """
    status = upload_manager.get_status(upload_id)

    if status.state != "completed":
        raise HTTPException(
            status_code=400, detail=f"Upload not ready for commit. Current state: {status.state}"
        )

    try:
        # Get all staged records
        staged_records = (
            db.query(SkyComponentStaging).filter(SkyComponentStaging.upload_id == upload_id).all()
        )

        if not staged_records:
            raise HTTPException(status_code=404, detail="No staged data found")

        # Get current max versions for all component_ids being committed
        component_ids = [r.component_id for r in staged_records]
        max_versions = {}

        if component_ids:
            # Query max version for each component_id (semantic version strings)
            version_results = (
                db.query(
                    SkyComponent.component_id, func.max(SkyComponent.version).label("max_version")
                )
                .filter(SkyComponent.component_id.in_(component_ids))
                .group_by(SkyComponent.component_id)
                .all()
            )

            max_versions = {r.component_id: r.max_version for r in version_results}

        # Helper function to increment minor version
        def increment_minor_version(version_str):
            """Increment minor version in semantic versioning (major.minor.patch).

            - If no existing version, start at 0.0.0 for first commit.
            - Subsequent commits increment the minor part: 0.0.0 -> 0.1.0 -> 0.2.0
            """
            if not version_str:
                return "0.0.0"
            try:
                parts = version_str.split(".")
                major = int(parts[0]) if len(parts) > 0 else 0
                minor = int(parts[1]) if len(parts) > 1 else 0
                patch = int(parts[2]) if len(parts) > 2 else 0
                return f"{major}.{minor + 1}.{patch}"
            except (ValueError, IndexError):
                return "0.0.0"

        # Copy from staging to main table with semantic version tracking
        for staged in staged_records:
            # Get next version for this component_id
            current_version = max_versions.get(staged.component_id, None)
            next_version = increment_minor_version(current_version)

            # Create main table record from staged data
            # Exclude 'id' and 'upload_id' from staging table fields
            record_data = {
                k: v for k, v in staged.columns_to_dict().items() if k not in ["id", "upload_id"]
            }
            record_data["version"] = next_version

            main_record = SkyComponent(**record_data)
            db.add(main_record)

            # Update max_versions for this component_id for subsequent records in same batch
            max_versions[staged.component_id] = next_version

        # Delete from staging
        db.query(SkyComponentStaging).filter(SkyComponentStaging.upload_id == upload_id).delete()

        db.commit()

        # Cleanup temp files
        upload_manager.cleanup(upload_id)

        logger.info("Successfully committed upload %s", upload_id)

        return {
            "status": "success",
            "message": f"Committed {len(staged_records)} records to main database",
            "records_committed": len(staged_records),
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
