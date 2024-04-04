"""
A simple fastAPI.
"""

from fastapi import FastAPI
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from starlette.middleware.cors import CORSMiddleware

from ska_sdp_global_sky_model.kubernetes.api.app.routers import point_sources

app = FastAPI()

session_local = sessionmaker(
    autocommit=False, autoflush=False, bind=point_sources.engine
)

Base = declarative_base()


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


app.include_router(point_sources.router)
