"""
Creates a DB session.
"""

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlmodel import create_engine

from ska_sdp_global_sky_model.kubernetes.api.app.config import DB_URL

engine = create_engine(DB_URL)

session_local = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()
