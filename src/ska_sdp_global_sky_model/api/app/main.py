# pylint: disable=no-member
"""
A simple fastAPI to obtain a local sky model from a global sky model.
"""

# pylint: disable=too-many-arguments, broad-exception-caught
import logging
import os
import tempfile
import time

<<<<<<< HEAD
from fastapi import Depends, FastAPI, UploadFile, HTTPException, File
from fastapi.responses import JSONResponse
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import ORJSONResponse
=======
from fastapi import BackgroundTasks, Depends, FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
>>>>>>> 1526961 (YAN-1801 ready for a test)
from sqlalchemy import text
from sqlalchemy.orm import Session
from starlette.middleware.cors import CORSMiddleware

from ska_sdp_global_sky_model.api.app.crud import get_local_sky_model
from ska_sdp_global_sky_model.api.app.ingest import get_full_catalog, post_process
from ska_sdp_global_sky_model.api.app.model import Source
from ska_sdp_global_sky_model.configuration.config import MWA, RACS, RCAL, Base, engine, get_db

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


@app.on_event("startup")
def create_db_and_tables():
    """
    Called on application startup.
    """
    logger.info("Creating the database and tables...")
    Base.metadata.create_all(engine)


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
async def upload_rcal(file: UploadFile = File(...), db: Session = Depends(get_db)):
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

        if file.content_type != "text/csv":
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
        with tempfile.NamedTemporaryFile(delete=True, suffix=".csv") as temp_file:
            temp_file_path = temp_file.name

            # Write the uploaded file to the temporary file
            contents = await file.read()
            file_size = len(contents)
            if file_size > free_space:
                raise HTTPException(status_code=400, detail="Insufficient disk space.")

            temp_file.write(contents)

            # Process the CSV data (example: print the path of the temporary file)
            print(f"Temporary file created at: {temp_file_path}")

            rcal_config = RCAL.copy()
            rcal_config["ingest"]["file_location"][0]["key"] = temp_file_path
            logger.info("Ingesting the catalogue...")

            if ingest(db, rcal_config):
                return JSONResponse(
                    content={"message": "RCAL uploaded and ingested successfully"},
                    status_code=200,
                )

            return JSONResponse(
                content={"message": "Error ingesting the catalogue (already present?)"},
                status_code=500,
            )
    except Exception as e:
        logger.error("Error on RCAL catalog ingest: %s", e)
        raise HTTPException(status_code=500, detail=str(e)) from e
