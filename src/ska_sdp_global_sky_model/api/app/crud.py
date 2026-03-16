# pylint: disable=no-member, too-many-locals, invalid-name,
# pylint: disable=too-few-public-methods, abstract-method, too-many-ancestors
"""
CRUD functionality goes here.
"""

import logging

from sqlalchemy.sql.functions import GenericFunction
from sqlalchemy.types import Boolean

from ska_sdp_global_sky_model.api.app.models import SkyComponent

logger = logging.getLogger(__name__)


class q3c_radial_query(GenericFunction):
    """SQLAlchemy function for h3c_radial_query(hpx, center, radius) -> BOOLEAN"""

    type = Boolean()
    inherit_cache = True
    name = "q3c_radial_query"


# pylint: disable-next=unused-argument,too-many-arguments,too-many-positional-arguments
def get_local_sky_model(
    db,
    ra_deg: float,
    dec_deg: float,
    fov_deg: float,
    catalogue_name: str,
    version: str | None = None,
) -> list:
    """
    Retrieves a local sky model (LSM) from a global sky model for a specific celestial observation.

    The LSM contains information about celestial components within a designated region of the sky.
    This function extracts this information from a database based on the provided
    right ascension (RA) and declination (Dec) coordinates.

    Args:
        db (Any): A database object containing the global sky model.
        ra (list[float]): A list containing right ascension values (in degrees).
        dec (list[float]): A list containing declination values (in degrees).
        fov (float): Field of view (in degrees).
        version (str): The version of the sky model which the LSM is selected from.

    Returns:
        list: A list containing the LSM data objects.
    """

    radius_deg = fov_deg / 2

    # Query sky components within radius
    query = db.query(SkyComponent).where(
        q3c_radial_query(
            SkyComponent.ra_deg,
            SkyComponent.dec_deg,
            ra_deg,
            dec_deg,
            radius_deg,
        )
    )

    if version:
        query = query.where(SkyComponent.version == version)

    query = query.where(SkyComponent.catalogue_name == catalogue_name)

    sky_components = query.all()

    return sky_components
