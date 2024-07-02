# pylint: disable=no-member
"""
A simple fastAPI to obtain a local sky model from a global sky model.
"""

# pylint: disable=too-many-arguments
import logging

from fastapi import BackgroundTasks, Depends, FastAPI
from sqlalchemy.orm import Session
from starlette.middleware.cors import CORSMiddleware

from ska_sdp_global_sky_model.api.app.crud import delete_previous_tiles, get_local_sky_model
from ska_sdp_global_sky_model.api.app.gleam_catalog import get_full_catalog, post_process
from ska_sdp_global_sky_model.api.app.model import Source
from ska_sdp_global_sky_model.configuration.config import Base, engine, get_db

logger = logging.getLogger(__name__)

app = FastAPI()

origins = []

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


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


@app.get("/ingest-gleam-catalog", summary="Create a point source for testing")
def point_source(db: Session = Depends(get_db)):
    """Ingesting the Gleam catalogue"""
    try:
        logger.info("Ingesting the Gleam catalogue...")
        if get_full_catalog(db):
            return "success"
        logger.error("Error (catalog already ingested)")
        return "Error (catalog already ingested)"
    except Exception as e:  # pylint: disable=broad-exception-caught
        return f"Error {e}"


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


@app.get("/local_sky_model")
async def get_local_sky_model_endpoint(
    ra: str,
    dec: str,
    flux_wide: float,
    telescope: str,
    fov: float,
    background_tasks: BackgroundTasks,
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
    background_tasks.add_task(delete_previous_tiles, db)
    return local_model
