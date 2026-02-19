# pylint: disable=no-member,too-few-public-methods, no-self-argument
"""
Configure variables to be used.
"""

import logging
from pathlib import Path

import ska_ser_logging
from fastapi.templating import Jinja2Templates
from sqlalchemy import create_engine, text
from sqlalchemy.ext.declarative import as_declarative, declared_attr
from sqlalchemy.orm import sessionmaker
from starlette.config import Config

ENV_FILE = Path(".env")
if not ENV_FILE.exists():
    ENV_FILE = None

config = Config(ENV_FILE)

template_path = Path(Path(__file__).parent.parent, "templates")
templates = Jinja2Templates(directory=template_path)

ska_ser_logging.configure_logging(level=config("SDP_LOG_LEVEL", default="WARNING").upper())
logger = logging.getLogger(__name__)
logger.info("Logging started for ska-sdp-global-sky-model-api")

# DB (Postgres)
DB_NAME: str = config("POSTGRES_DB_NAME", default="postgres")
POSTGRES_USER: str = config("POSTGRES_USER", default="postgres")
POSTGRES_PASSWORD: str = config("POSTGRES_PASSWORD", default="pass")
DB: str = config("POSTGRES_HOST", default="db")
DB_SCHEMA: str = config("POSTGRES_SCHEMA_NAME", default="public")
DB_URL = f"postgresql+psycopg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{DB}:5432/{DB_NAME}"

# HEALPix
NSIDE: int = config("NSIDE", default=4096)
NEST: bool = config("NEST", default="True").upper() == "TRUE"

REQUEST_WATCHER_TIMEOUT: int = int(config("REQUEST_WATCHER_TIMEOUT", default="30"))
SHARED_VOLUME_MOUNT: Path = Path(config("SHARED_VOLUME_MOUNT", default="/mnt/data"))


engine = create_engine(DB_URL)
session_local = sessionmaker(autocommit=False, autoflush=False, bind=engine)


@as_declarative()
class Base:
    """
    Declarative base.
    """

    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()


def get_db():
    """
    Provides a database session.

    Yields:
        sqlalchemy.orm.session.Session: A new database session object.
    """
    try:
        db = session_local()
        yield db
    finally:
        db.close()


def q3c_index():
    """Create Q3C extension + index exist"""
    with engine.begin() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS q3c;"))
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_source_q3c_ipix "
                'ON sky_component (version, q3c_ang2ipix("ra","dec"));'
            )
        )


# Standard catalogue metadata for all uploads
# Uses the standardized format with explicit column names
STANDARD_CATALOGUE_METADATA = {
    "version": "1.0.0",
    "description": "Standard sky survey catalogue ingestion metadata",
    "name": "Standard Sky Survey",
    "catalogue_name": "STANDARD",
    "ingest": {
        "file_location": [
            {
                "content": None,  # Content will be provided at runtime
            }
        ],
    },
}

# Backward compatibility alias
DEFAULT_CATALOGUE_METADATA = STANDARD_CATALOGUE_METADATA
