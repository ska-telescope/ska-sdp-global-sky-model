"""
CRUD functionality goes here.
"""

from sqlalchemy.sql.functions import GenericFunction
from sqlalchemy.types import Boolean


# TODO: is it ok to move this to request_responder.py?
class q3c_radial_query(GenericFunction):
    """SQLAlchemy function for q3c_radial_query(hpx, center, radius) -> BOOLEAN"""

    type = Boolean()
    inherit_cache = True
    name = "q3c_radial_query"
