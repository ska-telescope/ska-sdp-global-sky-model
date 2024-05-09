""" This module contains helper functions for the ska_sdp_global_sky_model """

from math import pi

from astropy import units as u
from astropy.coordinates import SkyCoord


def convert_ra_dec_to_skycoord(ra: float, dec: float, frame="icrs") -> SkyCoord:
    """
    Creates a SkyCoord object representing a celestial point in the ICRS frame.

    Args:
        ra (float): Right ascension of the point (J2000) in degrees.
        dec (float): Declination of the point (J2000) in degrees.
        frame (str, optional): The reference frame of the input coordinates. Defaults to "icrs"
        (ICRS).

    Returns:
        astropy.coordinates.SkyCoord: A SkyCoord object representing the celestial point in
        speficied frame (default = ICRS).

    Raises:
        ValueError: If RA or Dec values are outside valid ranges.

    """
    # Validate input values
    if not 0 <= ra <= 360:
        raise ValueError("RA must be between 0 and 360 degrees.")
    if not -90 <= dec <= 90:
        raise ValueError("Dec must be between -90 and 90 degrees.")

    # Create SkyCoord object in the specified frame (defaults to ICRS)

    # pylint: disable=no-member
    return SkyCoord(ra=ra * u.degree, dec=dec * u.degree, frame=frame)


def convert_arcminutes_to_radians(arcminutes: float) -> float:
    """Converts arcminutes to radians.

    Args:
        arcminutes: The value in arcminutes (float).

    Returns:
        The equivalent value in radians (float).

    Raises:
        TypeError: If the input `arcminutes` is not a number.
    """
    # Ensure input is a number
    if not isinstance(arcminutes, float):
        raise TypeError("Input must be a numeric value (float).")

    # Conversion factor: pi radians = 180 degrees, 1 degree = 60 arcminutes
    conversion_factor = pi / (180 * 60)
    return arcminutes * conversion_factor
