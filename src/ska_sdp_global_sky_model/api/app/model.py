"""
Data models for SQLAlchemy
"""

# pylint: disable=too-few-public-methods

from healpix_alchemy import Point, Tile
from sqlalchemy import Boolean, Column, Float, ForeignKey, Integer, String
from sqlalchemy.orm import mapped_column, Session
import json


from ska_sdp_global_sky_model.configuration.config import Base

class AOI(Base):
    __tablename__ = "AOI"
    id = mapped_column(Integer, primary_key=True, index=True, autoincrement=True)
    hpx = Column(Tile, index=True)


class Source(Base):
    """
    Model representing sources and their location.
    Derived from the wide (170-231MHz) image
    """

    __tablename__ = "Source"

    id = mapped_column(Integer, primary_key=True, index=True, autoincrement=True)
    name = Column(String, unique=True)
    RAJ2000 = Column(Float)
    RAJ2000_Error = Column(Float)
    DECJ2000 = Column(Float)
    DECJ2000_Error = Column(Float)
    Heal_Pix_Position = Column(Point, index=True)

    def to_json(self, session, telescope):
        wideband_data = self.get_widebanddata_by_source_id(session, self.id)
        if wideband_data.telescope == telescope:
            narrowband_data = self.get_narrowbanddata_by_source_id(session, self.id)
            return {
                "name": self.name,
                "location": (self.RAJ2000, self.DECJ2000),
                "wideband": json.dumps(wideband_data),
                "narrowband": json.dumps(narrowband_data),

            }
        else:
            {}
    
    def get_widebanddata_by_source_id(self, session: Session, source_id: int):
        """Retrieves a WideBandData record from the database based on source.id

        Args:
            session: A sqlalchemy session object
            source_id: The source.id of the WideBandData record to retrieve

        Returns:
            A WideBandData object containing the details, or None if not found
        """
        wideband_data = session.query(WideBandData).filter(WideBandData.source == source_id).first()

        if wideband_data:
            return wideband_data.__dict__
        else:
            return {}

    def get_narrowbanddata_by_source_id(self, session: Session, source_id: int):
        """Retrieves a NarrowBandData record from the database based on source.id

        Args:
            session: A sqlalchemy session object
            source_id: The source.id of the NarrowBandData record to retrieve

        Returns:
            A NarrowBandData object containing the details, or None if not found
        """
        narrowband_data = session.query(NarrowBandData).filter(NarrowBandData.source == source_id).first()

        if narrowband_data:
            return narrowband_data.__dict__
        else:
            return {}
        
    def get_telescope_source_id(self, session: Session, name: str):
        """Retrieves a ...
        """
        source_telescope = session.query(Telescope).filter(Telescope.name == name).first()

        if source_telescope:
        # Convert WideBandData object to dictionary
            source_telescope_dict = source_telescope.__dict__

        # Convert dictionary to JSON string
            return source_telescope_dict
        else:
            return {}

class Telescope(Base):
    """Model for Telescope which is the data source e.g. SKA Mid, SKA Low"""

    __tablename__ = "Telescope"

    id = mapped_column(Integer, primary_key=True, index=True, autoincrement=True)
    name = Column(String, unique=True)
    frequency_min = Column(Float)
    frequency_max = Column(Float)
    # ingested = Column(Boolean, default=False)


class Band(Base):
    """Model the bands that the sources were observed in"""

    __tablename__ = "Band"
    id = mapped_column(Integer, primary_key=True, index=True, autoincrement=True)
    centre = Column(Float)
    width = Column(Float)
    telescope = mapped_column(ForeignKey("Telescope.id"))


class NarrowBandData(Base):
    """The observed spectral information"""

    __tablename__ = "NarrowBandData"
    id = mapped_column(Integer, primary_key=True, index=True, autoincrement=True)
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

    source = mapped_column(ForeignKey("Source.id"))
    band = mapped_column(ForeignKey("Band.id"))


class WideBandData(Base):
    """Full Spectral band wide data"""

    __tablename__ = "WideBandData"
    id = mapped_column(Integer, primary_key=True, index=True, autoincrement=True)
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

    source = mapped_column(ForeignKey("Source.id"))
    telescope = mapped_column(ForeignKey("Telescope.id"))
