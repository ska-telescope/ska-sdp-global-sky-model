"""
Data models for SQLAlchemy
"""

# pylint: disable=too-few-public-methods

from healpix_alchemy import Point
from sqlalchemy import Column, Float, Integer, String
from sqlalchemy.orm import relationship

from ska_sdp_global_sky_model.api.app.config import Base


class Source(Base):
    """
    Model representing sources and their location.
    Derived from the wide (170-231MHz) image
    """

    __tablename__ = "sources"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    name = Column(String, unique=True)
    RAJ2000 = Column(Float)
    RAJ2000_Error = Column(Float)
    DecJ2000 = Column(Float)
    DecJ2000_Error = Column(Float)
    Heal_Pix_Position = Column(Point)


class Telescope(Base):
    """Model for Telescope which is the data source e.g. SKA Mid, SKA Low"""

    __tablename__ = "telescope"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    name = Column(String, unique=True)
    frequency_min = Column(Float)
    frequency_max = Column(Float)


class Band(Base):
    """Model the bands that the sources were observed in"""

    __tablename__ = "band"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    centre = Column(Float)
    width = Column(Float)
    telescope = relationship("Telescope", back_populates="band")


class NarrowBandData(Base):
    """The observed spectral information"""

    __tablename__ = "NarrowBandData"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    Bck_Narrow = Column(Float)
    Local_RMS_Narrow = Column(Float)
    Int_Flux_Narrow = Column(Float)
    Int_Flux_Narrow_Error = Column(Float)
    Resid_Mean_Narrow = Column(Float)
    Resid_Sd_Narrow = Column(Float)

    A_PSF_Narrow = Column(Float)
    B_PSF_Narrow = Column(Float)
    PA_PSF_Narrow = Column(Float)

    A_Narrow = Column(Float)
    A_Narrow_Error = Column(Float)
    B_Narrow = Column(Float)
    B_Narrow_Error = Column(Float)
    PA_Narrow = Column(Float)
    PA_Narrow_Error = Column(Float)
    Flux_Narrow = Column(Float)
    Flux_Narrow_Error = Column(Float)

    source = relationship("Source", back_populates="NarrowBandData")
    band = relationship("Source")


class WideBandData(Base):
    """Full Spectral band wide data"""

    __tablename__ = "WideBandData"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    Bck_Wide = Column(Float)
    Local_RMS_Wide = Column(Float)
    Int_Flux_Wide = Column(Float)
    Int_Flux_Wide_Error = Column(Float)
    Resid_Mean_Wide = Column(Float)
    Resid_Sd_Wide = Column(Float)
    Abs_Flux_Pct_Error = Column(Float)
    Fit_Flux_Pct_Error = Column(Float)

    A_PSF_Wide = Column(Float)
    B_PSF_Wide = Column(Float)
    PA_PSF_Wide = Column(Float)

    Spectral_Index = Column(Float)
    Spectral_Index_Error = Column(Float)

    A_Wide = Column(Float)
    A_Wide_Error = Column(Float)
    B_Wide = Column(Float)
    B_Wide_Error = Column(Float)
    PA_Wide = Column(Float)
    PA_Wide_Error = Column(Float)
    Flux_Wide = Column(Float)
    Flux_Wide_Error = Column(Float)

    telescope = relationship("Telescope")
    source = relationship("Source", back_populates="WideBandData")
