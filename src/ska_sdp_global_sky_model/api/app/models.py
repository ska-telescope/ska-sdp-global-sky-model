"""
SQLAlchemy ORM Models

This module defines database models using a hybrid approach:
- Source model: Dynamically generates columns from ska_sdp_datamodels.SkyComponent
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
    SkyComponent,
)
from sqlalchemy import BigInteger, Boolean, Column, Float, Integer, String
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.orm import mapped_column

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


def _add_dynamic_columns_to_model(model_class, dataclass):
    """
    Dynamically add columns from a dataclass to a SQLAlchemy model.

    This function iterates over the dataclass annotations and adds corresponding
    SQLAlchemy columns to the model class using setattr().

    Args:
        model_class: The SQLAlchemy model class to add columns to
        dataclass: The dataclass whose fields will be mapped to columns
    """
    for col, dtype in dataclass.__annotations__.items():
        if col == "name":
            # Special handling for name field - make it unique and not nullable
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


class Source(Base):
    """
    Represents a source in the sky model.

    This model dynamically generates columns from the SkyComponent dataclass,
    ensuring automatic synchronization with upstream data model changes.
    """

    __tablename__ = "source"
    __table_args__ = {"schema": DB_SCHEMA}

    # Hardcoded primary key
    id = mapped_column(Integer, primary_key=True, index=True, autoincrement=True)

    # Hardcoded database-specific field for spatial indexing
    healpix_index = Column(BigInteger, index=True, nullable=False)

    def columns_to_dict(self):
        """Return a dictionary representation of a row."""
        return {key: getattr(self, key) for key in self.__mapper__.c.keys()}


# Apply dynamic column generation to models
_add_dynamic_columns_to_model(GlobalSkyModelMetadata, GSMMetadataDataclass)
_add_dynamic_columns_to_model(Source, SkyComponent)
