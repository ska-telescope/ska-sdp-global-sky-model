"""
Data models for SQLAlchemy
"""

# pylint: disable=too-few-public-methods

import json

from healpix_alchemy import Point
from sqlalchemy import Boolean, Column, Float, ForeignKey, Integer, String
from sqlalchemy.dialects.postgresql import TEXT
from sqlalchemy.orm import Session, mapped_column

from ska_sdp_global_sky_model.configuration.config import Base


class AOI(Base):
    """Model for the Area of Interest table"""

    __tablename__ = "AOI"

    id = Column(Integer, primary_key=True, autoincrement=True)
    hpx = Column(String, index=True)  # Assuming HPX is a string identifier


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

    __tablename__ = "Source"

    id = mapped_column(Integer, primary_key=True, index=True, autoincrement=True)
    name = Column(String, unique=True)
    RAJ2000 = Column(Float)
    RAJ2000_Error = Column(Float)
    DECJ2000 = Column(Float)
    DECJ2000_Error = Column(Float)
    Heal_Pix_Position = Column(Point, index=True)
    sky_coord = Column(Point, index=True)
    json = Column(TEXT)

    def to_json(self, session: Session):
        """Serializes the source object and its associated telescope data to a JSON string.

        This method converts the current source object and its related telescope data
        into a JSON format. It includes basic source information like name, coordinates,
        and HealPix position. Additionally, it retrieves wideband and narrowband data
        associated with telescopes in the provided session.

        Args:
            session: A SQLAlchemy session object used to query the database.

        Returns:
            A JSON string representing the source object and its telescope data.

        Raises:
            No exception is raised by this method.
        """
        source_json = {
            "name": self.name,
            "coords_J2000": (self.RAJ2000, self.DECJ2000),
            "hpx": int(self.Heal_Pix_Position),
            "telescopes": {},
        }
        for telescope in session.query(Telescope).all():
            telescope_data = {
                "wideband": None,
                "narrowband": {},
            }

            wb_data = (
                session.query(WideBandData)
                .filter_by(telescope=telescope.id, source=self.id)
                .first()
            )

            if wb_data:
                telescope_data["wideband"] = json.dumps(wb_data.__dict__, default=lambda o: "")

            for band in session.query(Band).filter_by(telescope=telescope.id):
                nb_data = (
                    session.query(NarrowBandData).filter_by(band=band.id, source=self.id).first()
                )
                if nb_data:
                    telescope_data["narrowband"][band.centre] = json.dumps(
                        nb_data.__dict__, default=lambda o: ""
                    )

            if telescope_data["wideband"] or telescope_data["narrowband"]:
                source_json["telescopes"][telescope.name] = telescope_data

        return source_json

    def get_widebanddata_by_source_id(self, session: Session):
        """Retrieves a WideBandData record from the database based on source.id

        Args:
            session: A sqlalchemy session object
            source_id: The source.id of the WideBandData record to retrieve

        Returns:
            A WideBandData object containing the details, or None if not found
        """
        wideband_data = session.query(WideBandData).filter(WideBandData.source == f"{self.id}")

        if wideband_data:
            return wideband_data.__dict__
        return {}

    def get_narrowbanddata_by_source_id(self, session: Session):
        """Retrieves a NarrowBandData record from the database based on source.id

        Args:
            session: A sqlalchemy session object
            source_id: The source.id of the NarrowBandData record to retrieve

        Returns:
            A NarrowBandData object containing the details, or None if not found
        """
        narrowband_data = session.query(NarrowBandData).filter(NarrowBandData.source == self.id)

        if narrowband_data:
            return narrowband_data.__dict__
        return {}

    def get_telescope_source_id(self, session: Session, name: str):
        """Retrieves a ..."""
        source_telescope = session.query(Telescope).filter(Telescope.name == name).first()

        if source_telescope:
            # Convert WideBandData object to dictionary
            source_telescope_dict = source_telescope.__dict__

            # Convert dictionary to JSON string
            return source_telescope_dict
        return {}


class Telescope(Base):
    """Model for Telescope which is the data source e.g. SKA Mid, SKA Low"""

    __tablename__ = "Telescope"

    id = mapped_column(Integer, primary_key=True, index=True, autoincrement=True)
    name = Column(String, unique=True)
    frequency_min = Column(Float)
    frequency_max = Column(Float)
    ingested = Column(Boolean, default=False)


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
