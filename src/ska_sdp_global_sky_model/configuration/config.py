# pylint: disable=no-member,too-few-public-methods, no-self-argument
"""
Configure variables to be used.
"""

import logging
from pathlib import Path

import ska_ser_logging
from sqlalchemy import create_engine, text
from sqlalchemy.ext.declarative import as_declarative, declared_attr
from sqlalchemy.orm import sessionmaker
from starlette.config import Config

ENV_FILE = Path(".env")
if not ENV_FILE.exists():
    ENV_FILE = None

config = Config(ENV_FILE)

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
                'ON source (q3c_ang2ipix("ra","dec"));'
            )
        )


MWA = {
    "ingest": {
        "agent": "vizier",
        "key": "VIII/100",
        "wideband": True,
        "bands": [
            76,
            84,
            92,
            99,
            107,
            115,
            122,
            130,
            143,
            151,
            158,
            166,
            174,
            181,
            189,
            197,
            204,
            212,
            220,
            227,
        ],
    },
    "name": "Murchison Widefield Array",
    "catalog_name": "GLEAM",
    "frequency_min": 80,
    "frequency_max": 300,
    "source": "GLEAM",
    "bands": [
        76,
        84,
        92,
        99,
        107,
        115,
        122,
        130,
        143,
        151,
        158,
        166,
        174,
        181,
        189,
        197,
        204,
        212,
        220,
        227,
    ],
}


RACS = {
    "ingest": {
        "wideband": False,
        "agent": "file",
        "file_location": [
            {
                "key": "./datasets/AS110_Derived_Catalogue_racs_mid_components_v01_15373.csv",
                "bands": [1367],
                "heading_alias": {
                    "ra": "RAJ2000",
                    "e_ra": "e_RAJ2000",
                    "dec": "DEJ2000",
                    "e_dec": "e_DEJ2000",
                    "catalogue_id": "RACS",
                    "noise": "lrms1367",
                    "psf_pa": "psfPA1367",
                    "psf_min": "psfb1367",
                    "maj_axis": "a1367",
                    "min_axis": "b1367",
                    "psf_maj": "psfa1367",
                    "peak_flux": "Fp1367",
                    "e_peak_flux": "e_Fp1367",
                    "total_flux": "Fint1367",
                    "e_total_flux": "e_Fint1367",
                    "pa": "pa1367",
                },
                "heading_missing": ["resm1367", "resstd1367", "bck1367"],
            },
            {
                "key": "./datasets/AS110_Derived_Catalogue_racs_dr1_gaussians"
                "_galacticcut_v2021_08_v02_5723.csv",
                "bands": [887],
                "heading_alias": {
                    "ra": "RAJ2000",
                    "e_ra": "e_RAJ2000",
                    "dec": "DEJ2000",
                    "e_dec": "e_DEJ2000",
                    "catalogue_id": "RACS",
                    "noise": "lrms1367",
                    "psf_pa": "psfPA1367",
                    "psf_min": "psfb1367",
                    "maj_axis": "a1367",
                    "min_axis": "b1367",
                    "psf_maj": "psfa1367",
                    "peak_flux": "Fp1367",
                    "e_peak_flux": "e_Fp1367",
                    "total_flux": "Fint1367",
                    "e_total_flux": "e_Fint1367",
                    "pa": "pa1367",
                },
                "heading_missing": ["resm1367", "resstd1367", "bck1367"],
            },
        ],
    },
    "name": "ASKAP",
    "catalog_name": "RACS",
    "frequency_min": 700,
    "frequency_max": 1800,
    "source": "RACS",
    "bands": [887, 1367, 1632],
}
RCAL = {
    "ingest": {
        "wideband": True,
        "agent": "file",
        "file_location": [
            {
                "key": "unset",
                "heading_alias": {},
                "heading_missing": [],
                "bands": [
                    76,
                    84,
                    92,
                    99,
                    107,
                    115,
                    122,
                    130,
                    143,
                    151,
                    158,
                    166,
                    174,
                    181,
                    189,
                    197,
                    204,
                    212,
                    220,
                    227,
                ],
            }
        ],
    },
    "name": "Realtime Calibration test data",
    "catalog_name": "RCAL",
    "frequency_min": 80,
    "frequency_max": 300,
    "source": "GLEAM",
    "bands": [
        76,
        84,
        92,
        99,
        107,
        115,
        122,
        130,
        143,
        151,
        158,
        166,
        174,
        181,
        189,
        197,
        204,
        212,
        220,
        227,
    ],
}
