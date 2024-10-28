""" This module contains helper functions for the ska_sdp_global_sky_model """

from astropy.coordinates import SkyCoord
from numpy import pi
from sqlalchemy.orm import class_mapper


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
    if not (0.0 <= float(ra) and float(ra) <= 360.0):
        raise ValueError("RA must be between 0 and 360 degrees.")
    if not (-90.0 <= float(dec) and float(dec) <= 90.0):
        raise ValueError("Dec must be between -90 and 90 degrees.")
    # Create SkyCoord object in the specified frame (defaults to ICRS)
    # pylint: disable=no-member
    return SkyCoord(ra, dec, unit="deg", frame=frame)


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


def calculate_percentage(dividend: int | float, divisor: int | float) -> float:
    """
    Calculates the percentage that the `dividend` represents of the `divisor`
    (dividend/divisor)*100. If the `divisor` is zero the ZeroDivisionError is swallowed and
    it returns 0.0.

    Args:
        dividend (int|float): The value we want to express as a percentage of the total.
        divisor (int|float): The total value of which the `dividend` is a part.

    Returns:
        float: The percentage value between 0 and 100, rounded to two decimal places.

    Raises:
        ZeroDivisionError: If the `divisor` is zero.
    """
    if divisor == 0:
        # Handle division by zero case
        return 0.0
    percentage = (dividend / divisor) * 100
    return round(percentage, 2)  # Round to two decimal places


def model_to_dict(model):
    """Convert a SA row to dict."""
    columns = class_mapper(model.__class__).columns
    return {col.name: getattr(model, col.name) for col in columns}
