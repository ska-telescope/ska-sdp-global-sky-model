"""
A simple fastAPI to obtain a local sky model from a global sky model.
"""

import logging

import ska_ser_logging
from fastapi import Depends, FastAPI, Request
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import StreamingResponse
from fastapi_utils.tasks import repeat_every
from starlette.background import BackgroundTasks
from starlette.middleware.cors import CORSMiddleware

from ska_sdp_global_sky_model.api.crud import get_local_sky_model
from ska_sdp_global_sky_model.cli.common_cli import create_last_update
from ska_sdp_global_sky_model.configuration.config import (
    API_BASE_PATH,
    LOG_LEVEL,
    DataStore,
    get_ds,
)
from ska_sdp_global_sky_model.utilities.helper_functions import download_data_files

ska_ser_logging.configure_logging(LOG_LEVEL)

logger = logging.getLogger(__name__)

app = FastAPI(title="SKA SDP Global Sky Model", version="0.2.0", root_path=API_BASE_PATH)
app.add_middleware(GZipMiddleware, minimum_size=1000)

origins = []

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def load_data_first_time():
    """Force a reload of the Datastore data"""
    download_data_files()
    create_last_update()
    load_data()


def load_data():
    """Force a reload of the Datastore data"""
    get_ds().reload()


@app.on_event("startup")
async def startup_event():
    """Run startup tasks."""
    background_tasks = BackgroundTasks()
    background_tasks.add_task(load_data_first_time)
    await background_tasks()


@app.on_event("startup")
@repeat_every(seconds=60, logger=logger)
async def reload_data_set():
    """Run startup tasks."""
    background_tasks = BackgroundTasks()
    background_tasks.add_task(load_data)
    await background_tasks()


@app.get("/ping", summary="Ping the API")
def ping():
    """Returns {"ping": "live"} when called"""
    logger.debug("Ping: alive")
    return {"ping": "live"}


@app.get("/sources", summary="See all the point sources")
def get_point_sources(ds: DataStore = Depends(get_ds)):
    """Retrieve all point sources"""
    logger.info("Retrieving all point sources...")
    sources = ds.all()
    return sources.write_json()


# pylint: disable=too-many-arguments,too-many-positional-arguments
@app.get("/local_sky_model", response_class=StreamingResponse)
async def get_local_sky_model_endpoint(
    request: Request,
    ra: str,
    dec: str,
    telescope: str,
    fov: float,
    ds: DataStore = Depends(get_ds),
):
    """
    Get the local sky model from a global sky model.

    Args:
        ra (float): Right ascension of the observation point in degrees.
        dec (float): Declination of the observation point in degrees.
        flux_wide (float): Wide-field flux of the observation in Jy.
        telescope (str): Name of the telescope being used for the observation.
        fov (float): Field of view of the telescope in arcminutes.
        ds (DataStore):

    Returns:
        dict: A dictionary containing the local sky model information.

        The dictionary includes the following keys:
            - request: Allow users to free-form use search criteria
            - ra: The right ascension provided as input.
            - dec: The declination provided as input.
            - telescope: The telescope name provided as input.
            - fov: The field of view provided as input.
            - ds: ......
    """
    advanced_search = {}
    for key, value in request.query_params.items():
        if key in ["ra", "dec", "fov", "telescope"]:
            continue
        advanced_search[key] = value
    logger.info(
        "Requesting local sky model with the following parameters: ra:%s, \
dec:%s, telescope:%s, fov:%s, other options: %s",
        ra,
        dec,
        telescope,
        fov,
        advanced_search,
    )
    local_model = get_local_sky_model(
        ds, ra.split(";"), dec.split(";"), telescope, fov, advanced_search
    )
    return StreamingResponse(local_model.stream(), media_type="text/event-stream")
