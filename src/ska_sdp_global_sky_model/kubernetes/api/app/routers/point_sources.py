from sqlmodel import SQLModel
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker, scoped_session
from db import engine
from fastapi import APIRouter

router = APIRouter()


@router.on_event("startup")
def create_db_and_tables():
    SQLModel.metadata.create_all(engine)


@router.get("/ping", summary="Ping the API")
def ping():
    """Returns {"ping": "live"} when called"""
    return {"ping": "live"}

@router.get("/test", summary="Check we are connected to the database")
def test():
    Session = scoped_session(sessionmaker(bind=engine))
    s = Session()
    return s.execute(text('SELECT pg_sphere_version();'))