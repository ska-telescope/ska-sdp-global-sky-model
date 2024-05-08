"""
CRUD functionality goes here.
"""

from sqlalchemy import text
from sqlalchemy.orm import Session


def get_pg_sphere_version(db: Session):
    """
    Requests version information from pg_sphere.
    """
    return db.execute(text("SELECT pg_sphere_version();"))


def get_local_sky_model(
    ra: float,
    dec: float,
    flux_wide: float,
    telescope: str,
    field_of_view: float,
) -> dict:
    """
    Retrieves a local sky model from a global sky model for a given celestial observation.

    Args:
        ra (float): Right ascension of the observation point in degrees.
        dec (float): Declination of the observation point in degrees.
        flux_wide (float): Wide-field flux of the observation in Jy.
        telescope (str): Name of the telescope being used for the observation.
        field_of_view (float): Field of view of the telescope in degrees.

    Returns:
        dict: A dictionary containing the local sky model information.

        The dictionary includes the following keys:
            - ra: The right ascension provided as input.
            - dec: The declination provided as input.
            - flux_wide: The wide-field flux provided as input.
            - telescope: The telescope name provided as input.
            - field_of_view: The field of view provided as input.
            - local_data: ......
    """
    local_sky_model = {
        "ra": ra,
        "dec": dec,
        "flux_wide": flux_wide,
        "telescope": telescope,
        "field_of_view": field_of_view,
        "local_data": "placeholder",  # Replace with actual data from your function
    }
    return local_sky_model
