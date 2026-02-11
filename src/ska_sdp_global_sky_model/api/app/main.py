# pylint: disable=no-member, too-many-positional-arguments
"""
A simple fastAPI to obtain a local sky model from a global sky model.
"""

# pylint: disable=too-many-arguments, broad-exception-caught
import copy
import logging
import time
from contextlib import asynccontextmanager

from fastapi import BackgroundTasks, Depends, FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import ORJSONResponse
from sqlalchemy import text
from sqlalchemy.orm import Session
from starlette.middleware.cors import CORSMiddleware

from ska_sdp_global_sky_model.api.app.crud import get_local_sky_model
from ska_sdp_global_sky_model.api.app.ingest import ingest_catalog
from ska_sdp_global_sky_model.api.app.models import SkyComponent
from ska_sdp_global_sky_model.api.app.request_responder import start_thread
from ska_sdp_global_sky_model.api.app.upload_manager import UploadManager
from ska_sdp_global_sky_model.configuration.config import (
    STANDARD_CATALOG_CONFIG,
    engine,
    get_db,
)

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
    Run ingestion task in background.

    This function runs in a separate thread/task to keep the API responsive
    while processing large file uploads.

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

        # Get files from memory
        files_data = upload_manager.get_files(upload_id)

        # Ingest all files from memory
        for filename, content in files_data:
            # Deep copy to avoid modifying shared config
            file_config = copy.deepcopy(survey_config)
            # Pass content directly instead of file path
            file_config["ingest"]["file_location"][0]["content"] = content
            # Remove key since we're using content
            file_config["ingest"]["file_location"][0].pop("key", None)

            logger.info("Ingesting file from memory: %s", filename)
            if not ingest_catalog(db, file_config):
                raise RuntimeError(f"Ingest failed for {filename}")

        upload_manager.mark_completed(upload_id)
        logger.info("Background ingestion completed for upload %s", upload_id)

    except Exception as e:
        error_msg = str(e)
        logger.error("Background ingestion failed for upload %s: %s", upload_id, error_msg)
        upload_manager.mark_failed(upload_id, error_msg)

    finally:
        # Cleanup memory
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
        # Validate and save all files (validation happens in save_file)
        for file in files:
            await upload_manager.save_file(file, upload_status)

        # Schedule ingestion to run in background
        background_tasks.add_task(_run_ingestion_task, upload_id, survey_config)

        logger.info("Upload %s: files saved, ingestion scheduled in background", upload_id)

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
