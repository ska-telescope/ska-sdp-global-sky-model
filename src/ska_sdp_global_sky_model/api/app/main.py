"""
A simple fastAPI.
"""
from fastapi import FastAPI, Depends
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy import create_engine
from starlette.middleware.cors import CORSMiddleware

from ska_sdp_global_sky_model.api.app.config import DB_URL
from ska_sdp_global_sky_model.api.app import crud

engine = create_engine(DB_URL)
session_local = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

Base.metadata.create_all(bind=engine)

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
