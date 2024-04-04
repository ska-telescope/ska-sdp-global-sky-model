from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

import models
from db import engine
from routers import point_sources


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