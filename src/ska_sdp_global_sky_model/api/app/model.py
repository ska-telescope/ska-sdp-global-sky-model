"""
Data models for SQLAlchemy
"""

# pylint: disable=too-few-public-methods
# pylint: disable=no-member

import logging
from xmlrpc.client import DateTime

from sqlalchemy import BigInteger, Boolean, Column, Float, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import TEXT
from sqlalchemy.orm import Session, mapped_column

from ska_sdp_global_sky_model.configuration.config import DB_SCHEMA, Base

logger = logging.getLogger(__name__)


class Version(Base):
    """Model for GSM Version."""

    __table_args__ = {"schema": DB_SCHEMA}

    id = mapped_column(Integer, primary_key=True, index=True, autoincrement=True)
    name = Column(String, unique=True)
    number = Column(Integer)
    frequency_min = Column(Float)
    frequency_max = Column(Float)
    date_created = Column(DateTime)
    date_added = Column(DateTime)
    latest = Column(Boolean, default=False)

    __table_args__ = (
        # Enforce that versions aren't duplicated within the same name
        UniqueConstraint('name', 'number', name='_name_number_uc'),
        # Enforce that only one row can have lastest = True within the same name
        UniqueConstraint(
            'name',
            'latest',
            # This condition ensures the constraint only applies to rows where is_active is True
            postgresql_where=latest.is_(True)
        ),
    )



class Source(Base):
    """
    Represents a source of astronomical data and its location.

    This class is derived from a wideband image (170-231MHz).

    **Attributes:**
        name (str): Unique name of the source.
        coords (tuple): A tuple containing the Right Ascension (RAJ2000) and Declination
        (DECJ2000) of the source.
        hpx (int): Healpix index of the source's position.
        telescopes (dict): A dictionary containing data for telescopes that observed this source.
            Each key in the dictionary is the telescope name, and the corresponding value is
            another dictionary containing:
                * wideband (str or None): JSON-serialized data from the WideBandData table
                associated with this telescope and source, or None if no data exists.
                * narrowband (dict): A dictionary where each key is the center frequency of a
                narrowband observation and the value is JSON-serialized data from the
                NarrowBandData table for that specific band, telescope, and source, or None if
                no data exists.

    **Methods:**
        to_json(session) -> dict: Generates a dictionary containing the source's information and
        data from associated telescopes.
    """

    __table_args__ = {"schema": DB_SCHEMA}

    id = mapped_column(Integer, primary_key=True, index=True, autoincrement=True)
    name = Column(String, unique=True)
    RAJ2000 = Column(Float)
    DECJ2000 = Column(Float)
    Heal_Pix_Position = Column(BigInteger, index=True, nullable=False)
    I_76 = Column(Float)  # Integrated flux (Jy), centred at 76 MHz
    MajorAxis_76 = Column(Float)  # "a076" - Fitted semi-major axis (arcsec), centred at 76 MHz.
    MinorAxis_76 = Column(Float)  # "b076" - Fitted semi-minor axis (arcsec), centred at 76 MHz.
    Orientation_76 = Column(Float)  # "pa076" - Fitted position angle (deg), centred at 76 MHz.
    I_84 = Column(Float)
    MajorAxis_84 = Column(Float)
    MinorAxis_84 = Column(Float)
    Orientation_84 = Column(Float)
    I_92 = Column(Float)
    MajorAxis_92 = Column(Float)
    MinorAxis_92 = Column(Float)
    Orientation_92 = Column(Float)
    I_99 = Column(Float)
    MajorAxis_99 = Column(Float)
    MinorAxis_99 = Column(Float)
    Orientation_99 = Column(Float)
    I_107 = Column(Float)
    MajorAxis_107 = Column(Float)
    MinorAxis_107 = Column(Float)
    Orientation_107 = Column(Float)
    I_115 = Column(Float)
    MajorAxis_115 = Column(Float)
    MinorAxis_115 = Column(Float)
    Orientation_115 = Column(Float)
    I_122 = Column(Float)
    MajorAxis_122 = Column(Float)
    MinorAxis_122 = Column(Float)
    Orientation_122 = Column(Float)
    I_130 = Column(Float)
    MajorAxis_130 = Column(Float)
    MinorAxis_130 = Column(Float)
    Orientation_130 = Column(Float)
    I_143 = Column(Float)
    MajorAxis_143 = Column(Float)
    MinorAxis_143 = Column(Float)
    Orientation_143 = Column(Float)
    I_151 = Column(Float)
    MajorAxis_151 = Column(Float)
    MinorAxis_151 = Column(Float)
    Orientation_151 = Column(Float)
    I_158 = Column(Float)
    MajorAxis_158 = Column(Float)
    MinorAxis_158 = Column(Float)
    Orientation_158 = Column(Float)
    I_166 = Column(Float)
    MajorAxis_166 = Column(Float)
    MinorAxis_166 = Column(Float)
    Orientation_166 = Column(Float)
    I_174 = Column(Float)
    MajorAxis_174 = Column(Float)
    MinorAxis_174 = Column(Float)
    Orientation_174 = Column(Float)
    I_181 = Column(Float)
    MajorAxis_181 = Column(Float)
    MinorAxis_181 = Column(Float)
    Orientation_181 = Column(Float)
    I_189 = Column(Float)
    MajorAxis_189 = Column(Float)
    MinorAxis_189 = Column(Float)
    Orientation_189 = Column(Float)
    I_197 = Column(Float)
    MajorAxis_197 = Column(Float)
    MinorAxis_197 = Column(Float)
    Orientation_197 = Column(Float)
    I_204 = Column(Float)
    MajorAxis_204 = Column(Float)
    MinorAxis_204 = Column(Float)
    Orientation_204 = Column(Float)
    I_212 = Column(Float)
    MajorAxis_212 = Column(Float)
    MinorAxis_212 = Column(Float)
    Orientation_212 = Column(Float)
    I_220 = Column(Float)
    MajorAxis_220 = Column(Float)
    MinorAxis_220 = Column(Float)
    Orientation_220 = Column(Float)
    I_227 = Column(Float)
    MajorAxis_227 = Column(Float)
    MinorAxis_227 = Column(Float)
    Orientation_227 = Column(Float)
    version = mapped_column(ForeignKey(Version.id))
