# pylint: disable=no-member, too-many-locals, invalid-name, too-many-arguments
# pylint: disable=too-many-positional-arguments
"""
CRUD functionality goes here.
"""

import logging

import astropy.units as u
from astropy.coordinates import Latitude, Longitude, SkyCoord
from astropy_healpix import HEALPix

from ska_sdp_global_sky_model.configuration.config import NSIDE, NSIDE_PIXEL


def get_local_sky_model(
    ds,
    ra: list,
    dec: list,
    telescope: str,
    fov: float,
    advanced_search: dict,
):
    """
    Retrieves a local sky model (LSM) from a global sky model for a specific celestial observation.

    The LSM contains information about celestial sources within a designated region of the sky. \
        This function extracts this information from a database (`db`) based on the provided \
        right ascension (RA) and declination (Dec) coordinates.

    Args:
        ds (Any): A datastore object containing the global sky model.
        ra (list[float]): A list containing two right ascension values (in degrees) that define \
            the boundaries of the desired LSM region.
        dec (list[float]): A list containing two declination values (in degrees) that define the \
            boundaries of the desired LSM region.
        telescope (str): Placeholder for future implementation of the telescope name \
            being used for the observation. Currently not used.
        fov (float): Placeholder for future implementation of the telescope's field of\
            view (in arcminutes). Currently not used.
        advanced_search (dict): Advanced search parameters

    Returns:
        dict: A dictionary containing the LSM data. The structure of the dictionary is:

            {
                "region": {
                    "ra": List of RA coordinates (same as input `ra`).
                    "dec": List of Dec coordinates (same as input `dec`).
                },
                "count": Number of celestial sources found within the LSM region.
                "sources_in_area_of_interest": List of dictionaries, each representing a \
                    celestial source within the LSM region.
                    The structure of each source dictionary depends on the data model stored \
                        in the database (`db`).
            }
    """
    coord = SkyCoord(
        Longitude(float(ra[0]) * u.deg), Latitude(float(dec[0]) * u.deg), frame="icrs"
    )

    hp_f = HEALPix(nside=NSIDE, order="nested", frame="icrs")
    hp_pixel_fine = hp_f.cone_search_skycoord(coord, radius=float(fov) * u.deg)
    hp_c = HEALPix(nside=NSIDE_PIXEL, order="nested", frame="icrs")
    hp_pixel_course = hp_c.cone_search_skycoord(coord, radius=float(fov) * u.deg)

    # Modify the query to join the necessary tables
    result = ds.query_pxiels(
        {
            "healpix_pixel_rough": hp_pixel_course,
            "hp_pixel_fine": hp_pixel_fine,
            "telescopes": telescope.split(","),
            "advanced_search": advanced_search,
        }
    )
    return result


logger = logging.getLogger(__name__)
