"""
A simple fastAPI.
"""

from fastapi import Depends, FastAPI
from sqlalchemy.orm import Session
from starlette.middleware.cors import CORSMiddleware
from astropy.coordinates import SkyCoord
import astropy.units as u

from ska_sdp_global_sky_model.api.app import crud

from ska_sdp_global_sky_model.api.app.model import PointSource
from ska_sdp_global_sky_model.api.app.config import session_local, engine, Base

app = FastAPI()


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
    Base.metadata.create_all(engine)


@app.get("/ping", summary="Ping the API")
def ping():
    """Returns {"ping": "live"} when called"""
    return {"ping": "live"}


@app.get("/test", summary="Check we are connected to the database")
def test(db: Session = Depends(get_db)):
    """
    Requests version information from pg_sphere.
    """
    return crud.get_pg_sphere_version(db=db)


@app.get("/point-source-create", summary="Create a point source for testing")
def point_source(db: Session = Depends(get_db)):
    c3 = SkyCoord(1 * u.deg, 1 * u.deg)
    source = PointSource(name='TEST', point=c3)
    db.add(source)
    db.commit()


@app.get("/view-sources", summary="See all the point sources")
def get_point_sources(db: Session = Depends(get_db)):
    sources = db.query(PointSource).all()
    return sources
