"""
A simple fastAPI.
"""

import logging

from fastapi import Depends, FastAPI
from sqlalchemy.orm import Session
from starlette.middleware.cors import CORSMiddleware

from ska_sdp_global_sky_model.api.app import crud
from ska_sdp_global_sky_model.api.app.config import Base, engine, session_local

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
