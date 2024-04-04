"""
First endpoints.
"""

from fastapi import APIRouter
from sqlalchemy import text
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlmodel import SQLModel, create_engine

from ska_sdp_global_sky_model.kubernetes.api.app.config import DB_URL

router = APIRouter()

engine = create_engine(DB_URL)


@router.on_event("startup")
def create_db_and_tables():
    """
    Called on application startup.
    """
    SQLModel.metadata.create_all(engine)


@router.get("/ping", summary="Ping the API")
def ping():
    """Returns {"ping": "live"} when called"""
    return {"ping": "live"}


@router.get("/test", summary="Check we are connected to the database")
def test():
    """
    Requests version information from pg_sphere.
    """
    session = scoped_session(sessionmaker(bind=engine))
    s = session()
    return s.execute(text("SELECT pg_sphere_version();"))
