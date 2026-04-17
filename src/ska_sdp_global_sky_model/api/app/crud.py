"""
CRUD functionality goes here.
"""

import logging

from sqlalchemy.sql.functions import GenericFunction
from sqlalchemy.types import Boolean

from ska_sdp_global_sky_model.api.app.models import SkyComponent


class q3c_radial_query(GenericFunction):
    """SQLAlchemy function for q3c_radial_query(hpx, center, radius) -> BOOLEAN"""

    type = Boolean()
    inherit_cache = True
    name = "q3c_radial_query"
