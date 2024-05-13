"""
A simple fastAPI.
"""

import logging

from fastapi import Depends, FastAPI
from sqlalchemy.orm import Session
from starlette.middleware.cors import CORSMiddleware

from ska_sdp_global_sky_model.api.app import crud
from ska_sdp_global_sky_model.api.app.crud import get_local_sky_model
from ska_sdp_global_sky_model.api.app.gleam_catalog import get_full_catalog
from ska_sdp_global_sky_model.api.app.model import Source
from ska_sdp_global_sky_model.configuration.config import Base, engine, session_local

app = FastAPI()

logger = logging.getLogger(__name__)


def get_db():
    """
    Start a session.
    """
    try:
        db = session_local()
        yield db
    finally:
        db.close()


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
    logger.info("Creating the database and tables")
    Base.metadata.create_all(engine)


@app.get("/ping", summary="Ping the API")
def ping():
    """Returns {"ping": "live"} when called"""
    logger.info("Ping: alive")
    return {"ping": "live"}


@app.get("/test", summary="Check we are connected to the database")
def test(db: Session = Depends(get_db)):
    """
    Requests version information from pg_sphere.
    """
    return crud.get_pg_sphere_version(db=db)


@app.get("/ingest-gleam-catalog", summary="Create a point source for testing")
def point_source(db: Session = Depends(get_db)):
    """Import the Gleam catalogue"""
    try:
        if get_full_catalog(db):
            return "success"
        return "Error (catalog already ingested)"
    except Exception as e:  # pylint: disable=broad-exception-caught
        return f"Error {e}"


@app.get("/sources", summary="See all the point sources")
def get_point_sources(db: Session = Depends(get_db)):
    """Retrieve all point sources"""
    sources = db.query(Source).all()
    return [source.name for source in sources]


@app.get("/local_sky_model")
async def get_local_sky_model_endpoint(
    ra: list,
    dec: list,
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
    local_model = get_local_sky_model(db, ra, dec, flux_wide, telescope, fov)
    return local_model
