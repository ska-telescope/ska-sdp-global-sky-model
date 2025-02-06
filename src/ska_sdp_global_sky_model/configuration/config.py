# pylint: disable=no-member,too-few-public-methods, no-self-argument
"""
Configure variables to be used.
"""

import logging
from pathlib import Path

import ska_ser_logging
from starlette.config import Config

from ska_sdp_global_sky_model.configuration.datastore import DataStore

ENV_FILE: Path | None = Path(".env")
if not ENV_FILE.exists():
    ENV_FILE = None

config: Config = Config(ENV_FILE)

ska_ser_logging.configure_logging(
    logging.DEBUG if config("API_VERBOSE", default="false") == "true" else logging.INFO
)
logger = logging.getLogger(__name__)
logger.info("Logging started for ska-sdp-global-sky-model-api")

API_BASE_PATH: str = config("API_BASE_PATH", default="")
DATASET_ROOT: Path = Path(config("DATASET_ROOT", default="datasets/"))
TMDATA_SOURCE: str = config("TMDATA_SOURCE", default="")
TMDATA_KEYS: list[str] = config("TMDATA_KEYS", default="").split(",")


# HEALPix
NSIDE: int = config("NSIDE", default=128)
NSIDE_PIXEL: int = 16

DATASTORE: DataStore = DataStore(DATASET_ROOT)


def get_ds():
    """Get the datastore handle."""
    return DATASTORE


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
                "key": "ingest/AS110_Derived_Catalogue_racs_mid_components_v01_15373.csv",
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
                "key": "ingest/AS110_Derived_Catalogue_racs_dr1_gaussians_"
                "galacticcut_v2021_08_v02_5723.csv",
                "bands": [887],
                "heading_alias": {
                    "ra": "RAJ2000",
                    "e_ra": "e_RAJ2000",
                    "dec": "DEJ2000",
                    "e_dec": "e_DEJ2000",
                    "catalogue_id": "RACS",
                    "noise": "lrms887",
                    # "psf_pa": "psfPA887",
                    # "psf_min": "psfb887",
                    "maj_axis": "a887",
                    "min_axis": "b887",
                    # "psf_maj": "psfa887",
                    "peak_flux": "Fp887",
                    "e_peak_flux": "e_Fp887",
                    "total_flux_source": "Fint887",
                    "e_total_flux_source": "e_Fint887",
                    "pa": "pa887",
                },
                "heading_missing": [
                    "resm887",
                    "resstd887",
                    "bck887",
                    "psfa887",
                    "psfb887",
                    "psfPA887",
                    "Flux_Wide",
                ],
            },
        ],
    },
    "name": "ASKAP",
    "catalog_name": "RACS",
    "frequency_min": 700,
    "frequency_max": 1800,
    "source": "source_id",
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
