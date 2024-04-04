# pylint: disable=R0903
"""
File containing data models.
"""

from sqlalchemy import Column, Integer, String

from ska_sdp_global_sky_model.kubernetes.api.app.db import Base


class PointSource(Base):
    """
    Placeholder model class.
    """

    __tablename__ = "point_sources"

    id = Column(Integer, primary_key=True, index=True)
    RA = Column(String, default=True)
    DEC = Column(String, default=True)
