"""
SQLAlchemy ORM Models

This module defines database models:
- SkyComponent model: Dynamically generates columns from ska_sdp_datamodels.SkyComponent
  dataclass with hardcoded database-specific fields (healpix_index) and methods
- GlobalSkyModelMetadata: Dynamically generates columns from
  ska_sdp_datamodels.GlobalSkyModelMetadata dataclass with hardcoded methods

This ensures automatic adaptation to dataclass changes while maintaining control
over database-specific concerns like indexing and JSON serialization.
"""

# pylint: disable=too-few-public-methods
# pylint: disable=no-member

import logging
import typing
from typing import get_args, get_origin

from ska_sdp_datamodels.global_sky_model.global_sky_model import (
    GlobalSkyModelMetadata as GSMMetadataDataclass,
)
from ska_sdp_datamodels.global_sky_model.global_sky_model import (
    SkyComponent as SkyComponentDataclass,
)
from sqlalchemy import BigInteger, Boolean, Column, Float, Integer, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.orm import mapped_column
from sqlalchemy.sql import func
from sqlalchemy.types import DateTime

from ska_sdp_global_sky_model.configuration.config import DB_SCHEMA, Base

logger = logging.getLogger(__name__)


def _python_type_to_column(field_type: type) -> Column:
    """
    Convert a Python type annotation to a SQLAlchemy Column.

    All columns are nullable to support partial data insertion.

    Args:
        field_type: The Python type annotation from dataclass field

    Returns:
        SQLAlchemy Column instance with nullable=True
    """
    # Handle Optional types (Union[T, None])
    origin = get_origin(field_type)
    args = get_args(field_type)

    # Handle List types (e.g., List[Optional[float]])
    if origin is list:
        return Column(JSON, nullable=True)

    # Handle Union types (includes Optional which is Union[T, None])
    if origin is typing.Union or origin is type(None | int):
        non_none_types = [arg for arg in args if arg is not type(None)]
        if len(non_none_types) == 1:
            field_type = non_none_types[0]

    # Get the base type name
    type_name = field_type.__name__ if hasattr(field_type, "__name__") else str(field_type)

    # Map to SQLAlchemy type
    type_mapping = {
        "int": Integer,
        "float": Float,
        "str": String,
        "bool": Boolean,
        "list": JSON,
    }

    sa_type = type_mapping.get(type_name, String)

    # All fields are nullable to support partial data insertion
    return Column(sa_type, nullable=True)


def _add_dynamic_columns_to_model(model_class, dataclass, skip_columns=None):
    """
    Dynamically add columns from a dataclass to a SQLAlchemy model.

    This function iterates over the dataclass annotations and adds corresponding
    SQLAlchemy columns to the model class using setattr().

    Args:
        model_class: The SQLAlchemy model class to add columns to
        dataclass: The dataclass whose fields will be mapped to columns
        skip_columns: Set of column names to skip (already defined in the model)
    """
    if skip_columns is None:
        skip_columns = set()

    for col, dtype in dataclass.__annotations__.items():
        if col in skip_columns:
            continue  # Skip columns already defined in the model

        if col in ("name", "component_id"):
            # Special handling for name/component_id - make unique and not nullable
            setattr(model_class, col, Column(String, unique=True, nullable=False))
        else:
            # All other fields are nullable to support partial data insertion
            setattr(model_class, col, _python_type_to_column(dtype))


class GlobalSkyModelMetadata(Base):
    """Metadata describing a GSM catalogue instance."""

    __tablename__ = "globalskymodelmetadata"
    __table_args__ = {"schema": DB_SCHEMA}

    # Hardcoded primary key
    id = mapped_column(Integer, primary_key=True, index=True, autoincrement=True)

    def columns_to_dict(self):
        """Return a dictionary representation of a row."""
        return {key: getattr(self, key) for key in self.__mapper__.c.keys()}


class SkyComponent(Base):
    """
    Represents a sky component in the sky model.

    This model dynamically generates columns from the SkyComponent dataclass,
    ensuring automatic synchronization with upstream data model changes.

    Supports versioning - same component_id can have multiple versions.
    """

    __tablename__ = "sky_component"

    # Hardcoded primary key
    id = mapped_column(Integer, primary_key=True, index=True, autoincrement=True)

    # Hardcoded database-specific field for spatial indexing
    healpix_index = Column(BigInteger, index=True, nullable=False)

    # Version tracking - semantic versioning
    version = Column(String, nullable=False)

    # Add component_id explicitly so we can reference it in __table_args__
    component_id = Column(String, nullable=False, index=True)

    __table_args__ = (
        UniqueConstraint("component_id", "version", name="uq_component_version"),
        {"schema": DB_SCHEMA},
    )

    def columns_to_dict(self):
        """Return a dictionary representation of a row."""
        return {key: getattr(self, key) for key in self.__mapper__.c.keys()}


class SkyComponentStaging(Base):
    """
    Staging table for sky components awaiting manual verification.

    Data is first ingested here, then moved to SkyComponent after user commits.
    Each upload is independent - same component_id can exist in different uploads.
    """

    __tablename__ = "sky_component_staging"

    # Hardcoded primary key
    id = mapped_column(Integer, primary_key=True, index=True, autoincrement=True)

    # Hardcoded database-specific field for spatial indexing
    healpix_index = Column(BigInteger, index=True, nullable=False)

    # Track which upload batch this belongs to
    upload_id = Column(String, index=True, nullable=False)

    # Add component_id explicitly so we can reference it in __table_args__
    component_id = Column(String, nullable=False)

    __table_args__ = (
        UniqueConstraint("component_id", "upload_id", name="uq_component_upload"),
        {"schema": DB_SCHEMA},
    )

    def columns_to_dict(self):
        """Return a dictionary representation of a row."""
        return {key: getattr(self, key) for key in self.__mapper__.c.keys()}


# Apply dynamic column generation to models
_add_dynamic_columns_to_model(GlobalSkyModelMetadata, GSMMetadataDataclass)
# For main table, skip component_id since it's defined explicitly
# for the composite constraint with version
_add_dynamic_columns_to_model(SkyComponent, SkyComponentDataclass, skip_columns={"component_id"})
# For staging, skip component_id since it's defined explicitly
# for the composite constraint
_add_dynamic_columns_to_model(
    SkyComponentStaging, SkyComponentDataclass, skip_columns={"component_id"}
)


class CatalogMetadata(Base):
    """
    Catalog metadata table storing information about each catalog version.

    Each catalog upload has a single version that applies to all components
    in that catalog. The version must follow semantic versioning and increment
    from previous versions.
    """

    __tablename__ = "catalog_metadata"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    version = Column(String, nullable=False, unique=True, index=True)
    catalog_name = Column(String, nullable=False)
    description = Column(String, nullable=True)
    upload_id = Column(String, nullable=False, unique=True, index=True)
    # pylint: disable=not-callable
    uploaded_at = Column(DateTime, nullable=False, server_default=func.now())

    # Fields from GlobalSkyModelMetadata dataclass
    ref_freq = Column(Float, nullable=False, comment="Reference frequency in Hz")
    epoch = Column(String, nullable=False, comment="Epoch of observation")

    # Additional metadata fields
    author = Column(String, nullable=True)
    reference = Column(String, nullable=True)
    notes = Column(Text, nullable=True)

    def to_dict(self) -> dict:
        """Convert catalog metadata to dictionary."""
        return {
            "id": self.id,
            "version": self.version,
            "catalog_name": self.catalog_name,
            "description": self.description,
            "upload_id": self.upload_id,
            "uploaded_at": self.uploaded_at.isoformat() if self.uploaded_at else None,
            "ref_freq": self.ref_freq,
            "epoch": self.epoch,
            "author": self.author,
            "reference": self.reference,
            "notes": self.notes,
        }
