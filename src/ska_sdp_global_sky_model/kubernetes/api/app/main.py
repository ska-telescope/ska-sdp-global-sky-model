"""
A simple fastAPI.
"""

from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

from ska_sdp_global_sky_model.kubernetes.api.app import models
from ska_sdp_global_sky_model.kubernetes.api.app.db import engine
from ska_sdp_global_sky_model.kubernetes.api.app.routers import point_sources

models.Base.metadata.create_all(bind=engine)


app = FastAPI()


origins = []


app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


app.include_router(
    point_sources.router,
    prefix="/",
    tags=["point_sources"],
)
