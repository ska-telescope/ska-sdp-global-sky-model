# pylint: disable=no-member, too-many-positional-arguments
"""
A simple fastAPI to obtain a local sky model from a global sky model.
"""

# pylint: disable=too-many-arguments, broad-exception-caught
import logging
import os
import tempfile
import time
from typing import Optional

from fastapi import Depends, FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse, ORJSONResponse
from sqlalchemy import text
from sqlalchemy.orm import Session
from starlette.middleware.cors import CORSMiddleware

from ska_sdp_global_sky_model.api.app.crud import get_local_sky_model
from ska_sdp_global_sky_model.api.app.ingest import get_full_catalog, post_process
from ska_sdp_global_sky_model.api.app.model import Source
from ska_sdp_global_sky_model.api.app.request_responder import start_thread
from ska_sdp_global_sky_model.api.app.upload_manager import UploadManager
from ska_sdp_global_sky_model.configuration.config import (  # noqa # pylint: disable=unused-import
    MWA,
    RACS,
    RCAL,
    SKY_SURVEY,
    Base,
    engine,
    get_db,
)

logger = logging.getLogger(__name__)

app = FastAPI()
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


@app.on_event("startup")
async def startup_event():
    """Await for DB startup on app start"""
    wait_for_db()
    start_thread()


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


@app.get("/optimise-json", summary="Create a point source for testing")
def optimise_json(db: Session = Depends(get_db)):
    """Optimise the catalogue"""
    try:
        logger.debug("Optimising the catalogue...")
        if post_process(db):
            return "success"
        return "Error (catalog already ingested)"
    except Exception as e:  # pylint: disable=broad-exception-caught
        return f"Error {e}"


@app.get("/sources", summary="See all the point sources")
def get_point_sources(db: Session = Depends(get_db)):
    """Retrieve all point sources"""
    logger.info("Retrieving all point sources...")
    sources = db.query(Source).all()
    logger.info("Retrieved all point sources for all %s sources", str(len(sources)))
    source_list = []
    for source in sources:
        source_list.append([source.name, source.RAJ2000, source.DECJ2000])
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
        if file.content_type not in allowed_types and not file.filename.endswith('.csv'):
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


@app.post(
    "/upload-sky-survey-batch",
    summary="Upload sky survey CSV files in a batch",
    description="All sky survey CSV files must upload successfully or none are ingested.",
)
async def upload_sky_survey_batch(
    files: list[UploadFile] = File(...),
    db: Session = Depends(get_db),
    config: Optional[dict] = None,
):
    """
    Upload and ingest one or more sky survey CSV files atomically.

    All files are first written to a temporary staging directory. Ingestion
    only proceeds if all files are successfully uploaded. If any file
    fails validation or ingestion, the entire batch is rolled back.

    Parameters
    ----------
    files : list[UploadFile]
        One or more CSV files containing sky survey data.

    Raises
    ------
    HTTPException
        If validation, upload, or ingestion fails

    Returns
    -------
    dict
    db : Session
        Database session
    config : Optional[dict]
        Optional catalog configuration. If not provided, SKY_SURVEY config is used.

    Raises
    ------
    HTTPException
        If validation, upload, or ingestion fails

    Returns
    -------
    dict
        Upload identifier and completion status.
    """
    if not files:
        raise HTTPException(status_code=400, detail="No files provided")

    # Validate database connection
    try:
        db.execute(text("SELECT 1"))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Unable to access database") from e

    # Validate configuration
    survey_config = config if config else SKY_SURVEY.copy()
    if not survey_config:
        raise HTTPException(
            status_code=400,
            detail="SKY_SURVEY configuration not available. Please provide a config parameter.",
        )

    # Create upload tracking
    upload_status = upload_manager.create_upload(len(files))
    upload_id = upload_status.upload_id

    try:
        # Validate and save all files first
        for file in files:
            upload_manager.validate_file(file)
            await upload_manager.save_file(file, upload_status)

        # Ingest all files
        for file_path in upload_manager.list_files(upload_id):
            file_config = survey_config.copy()
            file_config["ingest"]["file_location"][0]["key"] = str(file_path)

            if not ingest(db, file_config):
                raise RuntimeError(f"Ingest failed for {file_path.name}")

        upload_manager.mark_completed(upload_id)

        return {"upload_id": upload_id, "status": "completed"}

    except HTTPException:
        upload_manager.mark_failed(upload_id, "HTTP exception during upload")
        raise

    except Exception as e:
        error_msg = str(e)
        upload_manager.mark_failed(upload_id, error_msg)
        raise HTTPException(
            status_code=500,
            detail=f"Sky survey upload failed: {error_msg}",
        ) from e

    finally:
        # Always cleanup temporary files
        upload_manager.cleanup(upload_id)


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
