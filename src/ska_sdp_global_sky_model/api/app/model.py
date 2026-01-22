"""
Data models for SQLAlchemy.

This module defines the database schema for the Global Sky Model (GSM).
It includes models for:
- Version: Catalog versions with metadata
- Source: Astronomical sources with coordinates and spectral data
"""

# pylint: disable=too-few-public-methods
# pylint: disable=no-member

import logging

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from ska_sdp_global_sky_model.configuration.config import DB_SCHEMA, Base

logger = logging.getLogger(__name__)


class Version(Base):
    """
    Model for GSM Version/Catalog metadata.

    Attributes:
        id (int): Primary key identifier.
        layer_id (str): Identifier for the catalog layer (e.g., 'gleam', 'nvss').
        version (str): Semantic version string (e.g., '1.0.0').
        epoch (DateTime): Observation epoch for the catalog.
        date_added (DateTime): Timestamp when this version was added to the database.
        default_version (bool): Flag indicating if this is the default version for the layer.
        catalogue_metadata (dict): JSON field for additional catalog-specific metadata.
        sources (relationship): Back-reference to Source objects belonging to this version.
    """

    __tablename__ = "version"
    __table_args__ = (
        # Enforce that versions aren't duplicated within the same layer
        UniqueConstraint("layer_id", "version", name="_layer_id_version_uc"),
        # TODO: Add partial unique constraint for default_version=True per layer_id
        # Requires: UniqueConstraint('layer_id', 'default_version',
        #           postgresql_where=(default_version.is_(True)))
        {"schema": DB_SCHEMA},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True, autoincrement=True)
    layer_id: Mapped[str] = mapped_column(String, nullable=False, default="default")
    version: Mapped[str] = mapped_column(String, nullable=False)
    epoch: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)
    date_added: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)
    default_version: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    catalogue_metadata: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    # Relationships
    sources: Mapped[list["Source"]] = relationship("Source", back_populates="version_ref")

    def __repr__(self) -> str:
        """Return string representation of Version."""
        return (
            f"<Version(id={self.id}, layer_id='{self.layer_id}', "
            f"version='{self.version}', default={self.default_version})>"
        )


class Source(Base):
    """
    Represents an astronomical source with positional and spectral data.

    This model stores source information including celestial coordinates,
    HEALPix spatial indexing, and frequency-dependent measurements.
    Currently contains GLEAM-specific frequency measurements that will be
    replaced with a more generic spectral model in future versions.

    Attributes:
        id (int): Primary key identifier.
        name (str): Unique source name/identifier.
        RAJ2000 (float): Right Ascension in J2000 coordinates (degrees).
        DECJ2000 (float): Declination in J2000 coordinates (degrees).
        Heal_Pix_Position (int): HEALPix index for spatial queries.
        version_id (int): Foreign key to Version table.
        version_ref (relationship): Reference to the associated Version object.

        # GLEAM-specific frequency measurements (LEGACY - to be replaced)
        I_* (float): Integrated flux density at specific frequencies (Jy).
        MajorAxis_* (float): Fitted semi-major axis at specific frequencies (arcsec).
        MinorAxis_* (float): Fitted semi-minor axis at specific frequencies (arcsec).
        Orientation_* (float): Position angle at specific frequencies (degrees).

    Notes:
        - RA/DEC are stored in degrees (J2000 epoch)
        - HEALPix position uses NSIDE=4096, NESTED scheme by default
        - Frequency-specific columns will be replaced by a generic spectral model
    """

    __tablename__ = "source"
    __table_args__ = (
        # Check constraints for coordinate validity
        CheckConstraint("RAJ2000 >= 0 AND RAJ2000 < 360", name="check_ra_range"),
        CheckConstraint("DECJ2000 >= -90 AND DECJ2000 <= 90", name="check_dec_range"),
        # Index on foreign key for performance (column name is "version" in DB)
        Index("ix_source_version_id", "version"),
        {"schema": DB_SCHEMA},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String, unique=True, nullable=False, index=True)
    RAJ2000: Mapped[float] = mapped_column(Float, nullable=False)
    DECJ2000: Mapped[float] = mapped_column(Float, nullable=False)
    Heal_Pix_Position: Mapped[int] = mapped_column(BigInteger, index=True, nullable=False)
    version_id: Mapped[int] = mapped_column(
        "version", Integer, ForeignKey(f"{Version.__table__.fullname}.id"), nullable=False
    )

    # Relationship
    version_ref: Mapped["Version"] = relationship("Version", back_populates="sources")

    # -------- START: GLEAM-specific columns (LEGACY - to be replaced) -----------#
    # These columns contain frequency-specific measurements from GLEAM catalog.
    # Future versions will replace this with a generic spectral model using
    # separate tables for fitted parameters (spectral indices, source shapes, etc.)
    I_76: Mapped[float | None] = mapped_column(Float, nullable=True)
    MajorAxis_76: Mapped[float | None] = mapped_column(Float, nullable=True)
    MinorAxis_76: Mapped[float | None] = mapped_column(Float, nullable=True)
    Orientation_76: Mapped[float | None] = mapped_column(Float, nullable=True)
    I_84: Mapped[float | None] = mapped_column(Float, nullable=True)
    MajorAxis_84: Mapped[float | None] = mapped_column(Float, nullable=True)
    MinorAxis_84: Mapped[float | None] = mapped_column(Float, nullable=True)
    Orientation_84: Mapped[float | None] = mapped_column(Float, nullable=True)
    I_92: Mapped[float | None] = mapped_column(Float, nullable=True)
    MajorAxis_92: Mapped[float | None] = mapped_column(Float, nullable=True)
    MinorAxis_92: Mapped[float | None] = mapped_column(Float, nullable=True)
    Orientation_92: Mapped[float | None] = mapped_column(Float, nullable=True)
    I_99: Mapped[float | None] = mapped_column(Float, nullable=True)
    MajorAxis_99: Mapped[float | None] = mapped_column(Float, nullable=True)
    MinorAxis_99: Mapped[float | None] = mapped_column(Float, nullable=True)
    Orientation_99: Mapped[float | None] = mapped_column(Float, nullable=True)
    I_107: Mapped[float | None] = mapped_column(Float, nullable=True)
    MajorAxis_107: Mapped[float | None] = mapped_column(Float, nullable=True)
    MinorAxis_107: Mapped[float | None] = mapped_column(Float, nullable=True)
    Orientation_107: Mapped[float | None] = mapped_column(Float, nullable=True)
    I_115: Mapped[float | None] = mapped_column(Float, nullable=True)
    MajorAxis_115: Mapped[float | None] = mapped_column(Float, nullable=True)
    MinorAxis_115: Mapped[float | None] = mapped_column(Float, nullable=True)
    Orientation_115: Mapped[float | None] = mapped_column(Float, nullable=True)
    I_122: Mapped[float | None] = mapped_column(Float, nullable=True)
    MajorAxis_122: Mapped[float | None] = mapped_column(Float, nullable=True)
    MinorAxis_122: Mapped[float | None] = mapped_column(Float, nullable=True)
    Orientation_122: Mapped[float | None] = mapped_column(Float, nullable=True)
    I_130: Mapped[float | None] = mapped_column(Float, nullable=True)
    MajorAxis_130: Mapped[float | None] = mapped_column(Float, nullable=True)
    MinorAxis_130: Mapped[float | None] = mapped_column(Float, nullable=True)
    Orientation_130: Mapped[float | None] = mapped_column(Float, nullable=True)
    I_143: Mapped[float | None] = mapped_column(Float, nullable=True)
    MajorAxis_143: Mapped[float | None] = mapped_column(Float, nullable=True)
    MinorAxis_143: Mapped[float | None] = mapped_column(Float, nullable=True)
    Orientation_143: Mapped[float | None] = mapped_column(Float, nullable=True)
    I_151: Mapped[float | None] = mapped_column(Float, nullable=True)
    MajorAxis_151: Mapped[float | None] = mapped_column(Float, nullable=True)
    MinorAxis_151: Mapped[float | None] = mapped_column(Float, nullable=True)
    Orientation_151: Mapped[float | None] = mapped_column(Float, nullable=True)
    I_158: Mapped[float | None] = mapped_column(Float, nullable=True)
    MajorAxis_158: Mapped[float | None] = mapped_column(Float, nullable=True)
    MinorAxis_158: Mapped[float | None] = mapped_column(Float, nullable=True)
    Orientation_158: Mapped[float | None] = mapped_column(Float, nullable=True)
    I_166: Mapped[float | None] = mapped_column(Float, nullable=True)
    MajorAxis_166: Mapped[float | None] = mapped_column(Float, nullable=True)
    MinorAxis_166: Mapped[float | None] = mapped_column(Float, nullable=True)
    Orientation_166: Mapped[float | None] = mapped_column(Float, nullable=True)
    I_174: Mapped[float | None] = mapped_column(Float, nullable=True)
    MajorAxis_174: Mapped[float | None] = mapped_column(Float, nullable=True)
    MinorAxis_174: Mapped[float | None] = mapped_column(Float, nullable=True)
    Orientation_174: Mapped[float | None] = mapped_column(Float, nullable=True)
    I_181: Mapped[float | None] = mapped_column(Float, nullable=True)
    MajorAxis_181: Mapped[float | None] = mapped_column(Float, nullable=True)
    MinorAxis_181: Mapped[float | None] = mapped_column(Float, nullable=True)
    Orientation_181: Mapped[float | None] = mapped_column(Float, nullable=True)
    I_189: Mapped[float | None] = mapped_column(Float, nullable=True)
    MajorAxis_189: Mapped[float | None] = mapped_column(Float, nullable=True)
    MinorAxis_189: Mapped[float | None] = mapped_column(Float, nullable=True)
    Orientation_189: Mapped[float | None] = mapped_column(Float, nullable=True)
    I_197: Mapped[float | None] = mapped_column(Float, nullable=True)
    MajorAxis_197: Mapped[float | None] = mapped_column(Float, nullable=True)
    MinorAxis_197: Mapped[float | None] = mapped_column(Float, nullable=True)
    Orientation_197: Mapped[float | None] = mapped_column(Float, nullable=True)
    I_204: Mapped[float | None] = mapped_column(Float, nullable=True)
    MajorAxis_204: Mapped[float | None] = mapped_column(Float, nullable=True)
    MinorAxis_204: Mapped[float | None] = mapped_column(Float, nullable=True)
    Orientation_204: Mapped[float | None] = mapped_column(Float, nullable=True)
    I_212: Mapped[float | None] = mapped_column(Float, nullable=True)
    MajorAxis_212: Mapped[float | None] = mapped_column(Float, nullable=True)
    MinorAxis_212: Mapped[float | None] = mapped_column(Float, nullable=True)
    Orientation_212: Mapped[float | None] = mapped_column(Float, nullable=True)
    I_220: Mapped[float | None] = mapped_column(Float, nullable=True)
    MajorAxis_220: Mapped[float | None] = mapped_column(Float, nullable=True)
    MinorAxis_220: Mapped[float | None] = mapped_column(Float, nullable=True)
    Orientation_220: Mapped[float | None] = mapped_column(Float, nullable=True)
    I_227: Mapped[float | None] = mapped_column(Float, nullable=True)
    MajorAxis_227: Mapped[float | None] = mapped_column(Float, nullable=True)
    MinorAxis_227: Mapped[float | None] = mapped_column(Float, nullable=True)
    Orientation_227: Mapped[float | None] = mapped_column(Float, nullable=True)
    # -------- END: GLEAM-specific columns (LEGACY) -----------#

    def __repr__(self) -> str:
        """Return string representation of Source."""
        return (
            f"<Source(id={self.id}, name='{self.name}', "
            f"RA={self.RAJ2000:.4f}, DEC={self.DECJ2000:.4f}, "
            f"version_id={self.version_id})>"
        )
