# pylint: disable=no-member, too-many-positional-arguments
"""
A simple fastAPI to obtain a local sky model from a global sky model.
"""

# pylint: disable=too-many-arguments, broad-exception-caught
import copy
import logging
import os
import tempfile
import time
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import BackgroundTasks, Depends, FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse, ORJSONResponse
from sqlalchemy import text
from sqlalchemy.orm import Session
from starlette.middleware.cors import CORSMiddleware

from ska_sdp_global_sky_model.api.app.crud import get_local_sky_model
from ska_sdp_global_sky_model.api.app.ingest import get_full_catalog
from ska_sdp_global_sky_model.api.app.models import Source
from ska_sdp_global_sky_model.api.app.request_responder import start_thread
from ska_sdp_global_sky_model.api.app.upload_manager import UploadManager
from ska_sdp_global_sky_model.configuration.config import (  # noqa # pylint: disable=unused-import
    CATALOG_CONFIGS,
    DEFAULT_CATALOG_CONFIG,
    MWA,
    RACS,
    RCAL,
    Base,
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


def ingest(db: Session, catalog_config: dict):
    """Ingest catalog"""
    try:
        if get_full_catalog(db, catalog_config):
            return True
        logger.error("Error ingesting the catalogue")
        return False
    except Exception as e:  # pylint: disable=broad-exception-caught
        raise e


@app.get("/ingest-gleam-catalog", summary="Ingest GLEAM {used in development}")
def ingest_gleam(db: Session = Depends(get_db)):
    """Ingesting the Gleam catalogue"""
    logger.info("Ingesting the Gleam catalogue...")
    return ingest(db, MWA)


@app.get("/ingest-racs-catalog", summary="Ingest RACS {used in development}")
def ingest_racs(db: Session = Depends(get_db)):
    """Ingesting the RACS catalogue"""
    logger.info("Ingesting the RACS catalogue...")
    return ingest(db, RACS)


@app.get("/sources", summary="See all the point sources")
def get_point_sources(db: Session = Depends(get_db)):
    """Retrieve all point sources"""
    logger.info("Retrieving all point sources...")
    sources = db.query(Source).all()
    logger.info("Retrieved all point sources for all %s sources", str(len(sources)))
    source_list = []
    for source in sources:
        source_list.append([source.name, source.ra, source.dec])
    return source_list


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


@app.post("/upload-rcal", summary="Ingest RCAL from a CSV {used in development}")
async def upload_rcal(
    file: UploadFile = File(...), db: Session = Depends(get_db), config: Optional[dict] = None
):
    """
    Uploads and processes an RCAL catalog file. This is a development endpoint.
    The file is expected to be a CSV file as exported from the GLEAM catalog.
    There is an example in the `tests/data` directory of this package.

    Parameters:
        file (UploadFile): The RCAL file to upload.

    Raises:
        HTTPException: If the file type is invalid or there is an error with the
        database session or disk space.

    Returns:
        JSONResponse: A success message if the RCAL file is uploaded and ingested successfully,
        or an error message if there is an issue with the catalog ingest.
    """
    try:
        # Accept common CSV mime types
        allowed_types = ["text/csv", "application/csv", "text/plain", "application/vnd.ms-excel"]
        if file.content_type not in allowed_types and not file.filename.endswith(".csv"):
            raise HTTPException(
                status_code=400, detail="Invalid file type. Please upload a CSV file."
            )

        try:
            db.execute(text("SELECT 1"))
        except Exception as e:
            raise HTTPException(status_code=500, detail="Unable to access database") from e

        # Check if there is sufficient disk space to write the file
        statvfs = os.statvfs("/")
        free_space = statvfs.f_frsize * statvfs.f_bavail

        # Create a temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as temp_file:
            temp_file_path = temp_file.name

            # Write the uploaded file to the temporary file
            contents = await file.read()
            file_size = len(contents)
            if file_size > free_space:
                raise HTTPException(status_code=400, detail="Insufficient disk space.")

            temp_file.write(contents)
            temp_file.flush()
            temp_file.close()
            # Process the CSV data (example: print the path of the temporary file)
            logger.info("Temporary file created at: %s, size: %d", temp_file_path, file_size)
            rcal_config = config
            if not rcal_config:
                rcal_config = RCAL.copy()

            rcal_config["ingest"]["file_location"][0]["key"] = temp_file_path
            logger.info("Ingesting the catalogue...")

            if ingest(db, rcal_config):
                return JSONResponse(
                    content={"message": "RCAL uploaded and ingested successfully"},
                    status_code=200,
                )

            os.remove(temp_file_path)

            return JSONResponse(
                content={"message": "Error ingesting the catalogue (already present?)"},
                status_code=500,
            )
    except Exception as e:
        logger.error("Error on file upload: %s", e)
        raise HTTPException(status_code=500, detail=str(e)) from e


def _get_catalog_config(config: Optional[dict], catalog: Optional[str]) -> dict:
    """
    Get catalog configuration for batch file uploads.

    For batch uploads, we need file-based configs with file_location structure.
    GLEAM uses Vizier by default, so we create a file-based config for it.

    Args:
        config: Custom configuration dictionary
        catalog: Predefined catalog name

    Returns:
        Catalog configuration dictionary with file_location structure

    Raises:
        HTTPException: If catalog name is invalid
    """
    if config:
        return config

    if catalog:
        if catalog not in CATALOG_CONFIGS:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown catalog '{catalog}'. Available: {list(CATALOG_CONFIGS.keys())}",
            )

        # For GLEAM, create a file-based config (it uses Vizier by default)
        if catalog == "GLEAM":
            base_config = copy.deepcopy(DEFAULT_CATALOG_CONFIG)
            base_config["catalog_name"] = "GLEAM"
            base_config["name"] = "GLEAM (Batch Upload)"
            base_config["source"] = "GLEAM"
            # GLEAM-specific column aliases for batch uploads
            # pylint: disable=duplicate-code
            base_config["ingest"]["file_location"][0]["heading_alias"] = {
                "Name": "Name",
                "RAJ2000": "RAJ2000",
                "DEJ2000": "DEJ2000",
                "Fpwide": "Fpwide",
                "Fintwide": "Fintwide",
                "awide": "awide",
                "bwide": "bwide",
                "pawide": "pawide",
                "alpha": "alpha",
            }
            # pylint: enable=duplicate-code
            return base_config

        return copy.deepcopy(CATALOG_CONFIGS[catalog])

    return copy.deepcopy(DEFAULT_CATALOG_CONFIG)


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

        # Note: CSV structure validation is skipped here as the ingestion process
        # will handle column mapping and report any issues. The validation was
        # complex due to different alias configurations per catalog.

        # Ingest all files
        file_paths = upload_manager.list_files(upload_id)
        for file_path in file_paths:
            # Deep copy to avoid modifying shared config
            file_config = copy.deepcopy(survey_config)
            file_config["ingest"]["file_location"][0]["key"] = str(file_path)

            logger.info("Ingesting file: %s", file_path.name)
            if not get_full_catalog(db, file_config):
                raise RuntimeError(f"Ingest failed for {file_path.name}")

        upload_manager.mark_completed(upload_id)
        logger.info("Background ingestion completed for upload %s", upload_id)

    except Exception as e:
        error_msg = str(e)
        logger.error("Background ingestion failed for upload %s: %s", upload_id, error_msg)
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
    config: Optional[dict] = None,
    catalog: Optional[str] = Form(None),
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
        One or more CSV files containing sky survey data.
    db : Session
        Database session
    config : Optional[dict]
        Optional catalog configuration dict. Takes precedence over catalog parameter.
    catalog : Optional[str]
        Name of predefined catalog config to use: 'GLEAM', 'RACS', 'RCAL', or 'GENERIC'.
        Defaults to 'GENERIC' if neither config nor catalog is provided.

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

    # Select and validate configuration
    survey_config = _get_catalog_config(config, catalog)

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
