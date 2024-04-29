from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.orm import relationship
from healpix_alchemy import Point

from ska_sdp_global_sky_model.api.app.config import Base


class PointSource(Base):
    """Model representing point sources and their location"""
    __tablename__ = "pointsources"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True)
    point = Column(Point)


class Telescope(Base):
    """Model for Telescope which is the data source e.g. SKA Mid, SKA Low"""
    __tablename__ = "telescope"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True)


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
    band = relationship("PointSource", back_populates="spectral")


# class SpectralWide(Base):
#     """Band wide spectral data"""
