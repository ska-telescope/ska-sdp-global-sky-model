"""
Data models for SQLAlchemy
"""

# pylint: disable=too-few-public-methods
# pylint: disable=no-member

import logging

from sqlalchemy import BigInteger, Boolean, Column, Float, ForeignKey, Integer, String
from sqlalchemy.dialects.postgresql import TEXT
from sqlalchemy.orm import Session, mapped_column

from ska_sdp_global_sky_model.configuration.config import DB_SCHEMA, Base

logger = logging.getLogger(__name__)


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


    def to_json(self, session: Session):
        """Serializes the source object and its associated telescope data to a JSON string.
        -
        -        This method converts the current source object and its related telescope data
        -        into a JSON format. It includes basic source information like name, coordinates,
        -        and HealPix position. Additionally, it retrieves wideband and narrowband data
        -        associated with telescopes in the provided session.
        -
        -        Args:
        -            session: A SQLAlchemy session object used to query the database.
        -
        -        Returns:
        -            A JSON string representing the source object and its telescope data.
        -
        -        Raises:
        -            No exception is raised by this method.
        -"""

        return {
            "name": self.name,
            "coords_J2000": (self.RAJ2000, self.DECJ2000),
            "hpx": int(self.Heal_Pix_Position),
            "narrow": self.get_narrowbanddata_by_source_id(session),
            "wide": self.get_widebanddata_by_source_id(session),
        }

    def get_widebanddata_by_source_id(self, session: Session):
        """Retrieves a WideBandData record from the database based on source.id

        Args:
            session: A sqlalchemy session object
            source_id: The source.id of the WideBandData record to retrieve

        Returns:
            A WideBandData object containing the details, or None if not found
        """
        wideband_data = session.query(WideBandData).filter_by(source=self.id).all()

        if wideband_data:
            return [wb.columns_to_dict() for wb in wideband_data]
        return {}

    def get_narrowbanddata_by_source_id(self, session: Session):
        """Retrieves a NarrowBandData record from the database based on source.id

        Args:
            session: A sqlalchemy session object
            source_id: The source.id of the NarrowBandData record to retrieve

        Returns:
            A NarrowBandData object containing the details, or None if not found
        """
        narrowband_data = session.query(NarrowBandData).filter_by(source=self.id).all()

        if narrowband_data:
            return [nb.columns_to_dict() for nb in narrowband_data]
        return {}

    def get_telescope_source_id(self, session: Session, name: str):
        """Retrieves a ..."""
        source_telescope = session.query(Telescope).filter(Telescope.name == name).first()

        if source_telescope:
            # Convert WideBandData object to dictionary
            source_telescope_dict = source_telescope.columns_to_dict()

            # Convert dictionary to JSON string
            return source_telescope_dict
        return {}


class Telescope(Base):
    """Model for Telescope which is the data source e.g. SKA Mid, SKA Low"""

    __table_args__ = {"schema": DB_SCHEMA}

    id = mapped_column(Integer, primary_key=True, index=True, autoincrement=True)
    name = Column(String, unique=True)
    frequency_min = Column(Float)
    frequency_max = Column(Float)
    ingested = Column(Boolean, default=False)

    def columns_to_dict(self):
        """
        Return a dictionary representation of a row.
        """
        dict_ = {}
        for key in self.__mapper__.c.keys():
            dict_[key] = getattr(self, key)
        return dict_


class Band(Base):
    """Model the bands that the sources were observed in"""

    __table_args__ = {"schema": DB_SCHEMA}
    id = mapped_column(Integer, primary_key=True, index=True, autoincrement=True)
    centre = Column(Float)
    width = Column(Float)
    telescope = mapped_column(ForeignKey(Telescope.id))


class NarrowBandData(Base):
    """The observed spectral information"""

    __table_args__ = {"schema": DB_SCHEMA}
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

    Polarised = Column(Boolean)
    Stokes = Column(String)
    Rotational_Measure = Column(Float)
    Rotational_Measure_Error = Column(Float)
    Fractional_Polarisation = Column(Float)
    Fractional_Polarisation_Error = Column(Float)
    Faraday_Complex = Column(Boolean)

    Spectral_Index = Column(Float)
    Spectral_Index_Error = Column(Float)

    Variable = Column(Boolean)
    Modulation_Index = Column(Float)
    Debiased_Modulation_Index = Column(Float)

    source = mapped_column(ForeignKey(Source.id))
    band = mapped_column(ForeignKey(Band.id))

    def columns_to_dict(self):
        """
        Return a dictionary representation of a row.
        """
        dict_ = {}
        for key in self.__mapper__.c.keys():
            dict_[key] = getattr(self, key)
        return dict_


class WideBandData(Base):
    """Full Spectral band wide data"""

    __table_args__ = {"schema": DB_SCHEMA}
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

    Polarised = Column(Boolean)
    Stokes = Column(String)
    Rotational_Measure = Column(Float)
    Rotational_Measure_Error = Column(Float)
    Fractional_Polarisation = Column(Float)
    Fractional_Polarisation_Error = Column(Float)
    Faraday_Complex = Column(Boolean)

    Spectral_Index = Column(Float)
    Spectral_Index_Error = Column(Float)

    Spectral_Curvature = Column(Float)
    Spectral_Curvature_Error = Column(Float)

    Variable = Column(Boolean)
    Modulation_Index = Column(Float)
    Debiased_Modulation_Index = Column(Float)

    source = mapped_column(ForeignKey(Source.id))
    telescope = mapped_column(ForeignKey(Telescope.id))

    def columns_to_dict(self):
        """
        Return a dictionary representation of a row.
        """
        dict_ = {}
        for key in self.__mapper__.c.keys():
            dict_[key] = getattr(self, key)
        return dict_
