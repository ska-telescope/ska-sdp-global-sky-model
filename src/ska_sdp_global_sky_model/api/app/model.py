"""
Data models for SQLAlchemy
"""

# pylint: disable=too-few-public-methods

from healpix_alchemy import Point
from sqlalchemy import Column, Float, Integer, String
from sqlalchemy.orm import relationship

from ska_sdp_global_sky_model.api.app.config import Base


class PointSource(Base):
    """Model representing point sources and their location"""

    __tablename__ = "pointsources"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True)
    point = Column(Point)
    A_Wide = Column(Float)
    A_Wide_Error = Column(Float)
    B_Wide = Column(Float)
    B_Wide_Error = Column(Float)


class Telescope(Base):
    """Model for Telescope which is the data source e.g. SKA Mid, SKA Low"""

    __tablename__ = "telescope"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True)
    frequency_min = Column(Float)
    frequency_max = Column(Float)


class Band(Base):
    """Model the bands that the sources were observed in"""

    __band__ = "band"
    id = Column(Integer, primary_key=True, index=True)
    centre = Column(Float)
    width = Column(Float)
    telescope = relationship("Telescope", back_populates="band")


class Spectral(Base):
    """The observed spectral information"""

    _tablename__ = "spectral"
    id = Column(Integer, primary_key=True, index=True)
    background = Column(Float)
    rms_noise = Column(Float)
    flux = Column(Float)
    source = relationship("PointSource", back_populates="spectral")
    band = relationship("PointSource")


class Spectrum(Base):
    """Full Spectral band wide data"""

    _tablename__ = "spectrum"
    id = Column(Integer, primary_key=True, index=True)
    Bck_Wide = Column(Float)
    Local_RMS_Wide = Column(Float)
    Flux_Wide = Column(Float)
    Flux_Wide_Error = Column(Float)
    Int_Flux_Wide = Column(Float)
    Int_Flux_Wide_Error = Column(Float)
    PA_Wide = Column(Float)
    PA_Wide_Error = Column(Float)
    Resid_Mean_Wide = Column(Float)
    Resid_Sd_Wide = Column(Float)
    Abs_Flux_Pct_Error = Column(Float)
    Fit_Flux_Pct_Error = Column(Float)
    A_PSF_Wide = Column(Float)
    B_PSF_Wide = Column(Float)
    PA_PSF_Wide = Column(Float)
    telescope = relationship("Telescope")
    source = relationship("PointSource", back_populates="spectrum")
