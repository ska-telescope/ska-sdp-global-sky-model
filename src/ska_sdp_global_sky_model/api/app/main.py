# pylint: disable=no-member, too-many-positional-arguments
"""
A simple fastAPI to obtain a local sky model from a global sky model.
"""

# pylint: disable=too-many-arguments, broad-exception-caught
import logging
import time

from fastapi import Depends, FastAPI, File, HTTPException, UploadFile, Request
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse, ORJSONResponse, HTMLResponse
from sqlalchemy import text
from sqlalchemy.orm import Session
from starlette.middleware.cors import CORSMiddleware

from ska_sdp_global_sky_model.api.app.crud import get_local_sky_model
from ska_sdp_global_sky_model.api.app.models import SkyComponent
from ska_sdp_global_sky_model.api.app.request_responder import start_thread
from ska_sdp_global_sky_model.configuration.config import (  # noqa # pylint: disable=unused-import
    MWA,
    RACS,
    RCAL,
    Base,
    engine,
    get_db,
    templates,
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


@app.get("/sources",  response_class=HTMLResponse, summary="See all the point sources")
def get_point_sources(request: Request, db: Session = Depends(get_db)):
    """Retrieve all point sources"""
    logger.info("Retrieving all point sources...")
    components = db.query(SkyComponent).all()
    logger.info("Retrieved all point sources for all %s components", str(len(components)))
    return templates.TemplateResponse("table.html", {"request": request, "items": list(components)})


@app.get("/local_sky_model", response_class=HTMLResponse)
async def get_local_sky_model_endpoint(
    request: Request,
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
        request (Request): HTTP request object.
        ra (float): Right ascension of the observation point in degrees.
        dec (float): Declination of the observation point in degrees.
        flux_wide (float): Wide-field flux of the observation in Jy.
        telescope (str): Name of the telescope being used for the observation.
        fov (float): Field of view of the telescope in arcminutes.
        db (Session): Database session object.

    Returns:
        html: An HTML template response.

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
    return templates.TemplateResponse("table.html", {"request": request, "items": list(local_model)})
