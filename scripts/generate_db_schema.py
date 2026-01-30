#!/usr/bin/env python3
"""
Generate SQLAlchemy database schema from ska_sdp_datamodels dataclasses.

This script dynamically generates the db_schema.py file by introspecting
the dataclasses defined in ska_sdp_datamodels.global_sky_model.global_sky_model
and mapping them to appropriate SQLAlchemy models with correct column types.

The script is fully dynamic and will adapt to any changes in the datamodel
structure without requiring modifications to the generator code.
"""

import argparse
import importlib
import importlib.metadata
import inspect
import logging
import sys
import typing
from dataclasses import fields, is_dataclass
from datetime import datetime
from pathlib import Path
from typing import get_args, get_origin

# Add parent directory to path to allow imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def get_package_version(package_name: str) -> str:
    """
    Get the version of an installed package.

    Args:
        package_name: Name of the package

    Returns:
        Version string or "unknown" if not found
    """
    try:
        return importlib.metadata.version(package_name)
    except Exception:  # pylint: disable=broad-exception-caught
        return "unknown"


# Mapping from Python types to SQLAlchemy column types
TYPE_MAPPING = {
    "int": "Integer",
    "float": "Float",
    "str": "String",
    "bool": "Boolean",
    "list": "JSON",  # For List types like spec_idx
}

# Special column name mappings for backward compatibility with existing database
COLUMN_NAME_MAPPING = {
    "SkySource": {
        "name": "name",
        "ra": "RAJ2000",
        "dec": "DECJ2000",
        "i_pol": "I_Pol",
        "major_ax": "Major_Ax",
        "minor_ax": "Minor_Ax",
        "pos_ang": "Pos_Ang",
        "spec_idx": "Spec_Idx",
        "log_spec_idx": "Log_Spec_Idx",
        "spec_curv": "Spec_Curv",
        "q_pol": "Q_Pol",
        "u_pol": "U_Pol",
        "v_pol": "V_Pol",
        "pol_frac": "Pol_Frac",
        "pol_ang": "Pol_Ang",
        "rot_meas": "Rot_Meas",
    }
}

# Configuration for specific models
MODEL_CONFIG = {
    "SkySource": {
        "table_name": "Source",
        "skip_fields": set(),  # No container fields in new model
        "additional_fields": {
            "RAJ2000_Error": ("Float", {"nullable": True}),
            "DECJ2000_Error": ("Float", {"nullable": True}),
            "Heal_Pix_Position": ("BigInteger", {"index": True, "nullable": False}),
            "json": ("TEXT", {}),
        },
        "unique_fields": {"name"},
        "custom_methods": True,
    },
}


def discover_dataclasses(module_name: str) -> dict:
    """
    Discover all dataclasses in a given module.

    Args:
        module_name: Fully qualified module name to inspect

    Returns:
        Dictionary mapping class names to class objects
    """
    try:
        module = importlib.import_module(module_name)
    except ImportError as e:
        logger.error("Failed to import module %s: %s", module_name, e)
        sys.exit(1)

    dataclasses = {}
    for name, obj in inspect.getmembers(module):
        if is_dataclass(obj) and not name.startswith("_"):
            dataclasses[name] = obj
            logger.debug("Discovered dataclass: %s", name)

    return dataclasses


def infer_relationships(dataclasses: dict) -> dict:
    """
    Infer relationships between dataclasses based on field types.

    Args:
        dataclasses: Dictionary of dataclass name to class object

    Returns:
        Dictionary mapping class names to their relationship information
    """
    relationships = {}

    for class_name, cls in dataclasses.items():
        relationships[class_name] = {
            "foreign_keys": {},
            "containers": {},
            "base_class": None,
        }

        # Check for base classes
        for base in cls.__bases__:
            if is_dataclass(base) and base.__name__ in dataclasses:
                relationships[class_name]["base_class"] = base.__name__
                logger.debug("%s extends %s", class_name, base.__name__)

        # Analyze fields for relationships
        for field in fields(cls):
            field_type = field.type
            origin = get_origin(field_type)
            args = get_args(field_type)

            # Handle list[SomeClass] or dict[K, SomeClass] - these are container relationships
            if origin in (list, dict):
                for arg in args:
                    if hasattr(arg, "__name__") and arg.__name__ in dataclasses:
                        relationships[class_name]["containers"][field.name] = arg.__name__
                        logger.debug(
                            "%s.%s is a container of %s", class_name, field.name, arg.__name__
                        )

            # Handle Optional types that might reference dataclasses
            elif origin is type(None | int):
                for arg in args:
                    if (
                        hasattr(arg, "__name__")
                        and arg.__name__ in dataclasses
                        and arg is not type(None)
                    ):
                        relationships[class_name]["foreign_keys"][field.name] = arg.__name__
                        logger.debug(
                            "%s.%s optionally references %s",
                            class_name,
                            field.name,
                            arg.__name__,
                        )

    return relationships


def python_type_to_sqlalchemy(field_type: type, field_name: str) -> tuple[str, dict]:
    """
    Convert Python type annotation to SQLAlchemy column type.

    Args:
        field_type: The Python type annotation
        field_name: Name of the field (for context)

    Returns:
        Tuple of (SQLAlchemy type string, column kwargs dict)
    """
    column_kwargs = {}

    # Handle Optional types (e.g., str | None or Optional[str])
    origin = get_origin(field_type)
    args = get_args(field_type)

    # Handle List types (e.g., List[Optional[float]])
    if origin is list:
        logger.debug("Field %s is a List type, using JSON column", field_name)
        column_kwargs["nullable"] = True
        return "JSON", column_kwargs

    # Handle Union types (includes Optional which is Union[T, None])
    if origin is typing.Union or origin is type(None | int):
        # Extract non-None types
        non_none_types = [arg for arg in args if arg is not type(None)]
        if len(non_none_types) == 1:
            field_type = non_none_types[0]
            column_kwargs["nullable"] = True
        else:
            raise ValueError(f"Complex union types not supported for field {field_name}")

    # Get the base type name
    type_name = field_type.__name__ if hasattr(field_type, "__name__") else str(field_type)

    # Map to SQLAlchemy type
    if type_name in TYPE_MAPPING:
        sqlalchemy_type = TYPE_MAPPING[type_name]
    else:
        logger.warning("Unknown type %s for field %s, defaulting to String", type_name, field_name)
        sqlalchemy_type = "String"

    return sqlalchemy_type, column_kwargs


def get_column_name(class_name: str, field_name: str, suffix: str = "") -> str:
    """
    Get the database column name for a field.

    Args:
        class_name: Name of the class
        field_name: Name of the field
        suffix: Optional suffix to add (e.g., 'narrow', 'wide')

    Returns:
        The database column name
    """
    # Check for explicit mapping
    if class_name in COLUMN_NAME_MAPPING:
        if field_name in COLUMN_NAME_MAPPING[class_name]:
            return COLUMN_NAME_MAPPING[class_name][field_name]

    # Apply suffix if provided with proper capitalization
    # Convert field_name to PascalCase with underscores (e.g., flux -> Flux, int_flux -> Int_Flux)
    if suffix:
        # Split by underscore and capitalize each part
        parts = field_name.split("_")
        capitalized = "_".join(part.capitalize() for part in parts)
        # Capitalize the suffix as well
        return f"{capitalized}_{suffix.capitalize()}"

    return field_name


def should_skip_field(field_name: str, class_name: str, relationships: dict) -> bool:
    """
    Determine if a field should be skipped in SQL generation.

    Args:
        field_name: Name of the field
        class_name: Name of the class containing this field
        relationships: Relationship information

    Returns:
        True if field should be skipped
    """
    # Check config
    config = MODEL_CONFIG.get(class_name, {})
    if field_name in config.get("skip_fields", set()):
        return True

    # Skip if it's in configured foreign keys (we handle those separately)
    if field_name in config.get("foreign_keys", {}):
        return True

    # Skip container fields (lists, dicts of other dataclasses)
    if field_name in relationships.get(class_name, {}).get("containers", {}):
        return True

    # Don't skip List/dict of primitive types - those can be stored as JSON
    # Only skip complex types we can't map (list/dict of dataclasses are already handled above)

    return False


def generate_model_from_dataclass(  # pylint: disable=too-many-locals,too-many-branches,too-many-statements
    class_name: str, dataclass_obj: type, relationships: dict, dataclasses: dict
) -> str:
    """
    Generate SQLAlchemy model code from a dataclass dynamically.

    Args:
        class_name: Name of the dataclass
        dataclass_obj: The dataclass object
        relationships: Dictionary of relationship information
        dataclasses: All discovered dataclasses

    Returns:
        String containing the model class definition
    """
    config = MODEL_CONFIG.get(class_name, {})
    table_name = config.get("table_name", class_name)
    suffix = config.get("suffix", "")
    additional_fields = config.get("additional_fields", {})
    unique_fields = config.get("unique_fields", set())
    configured_fks = config.get("foreign_keys", {})

    lines = []
    lines.append(f"class {table_name}(Base):")

    # Use docstring from dataclass if available
    doc = inspect.getdoc(dataclass_obj)
    if doc:
        # Only use first line or paragraph
        doc_lines = doc.split("\n")
        first_para = []
        for line in doc_lines:
            if line.strip():
                first_para.append(line.strip())
            elif first_para:
                break
        if first_para:
            lines.append(f'    """{" ".join(first_para)}"""')
        else:
            lines.append(f'    """Model for {table_name}"""')
    else:
        lines.append(f'    """Model for {table_name}"""')

    lines.append("")
    lines.append('    __table_args__ = {"schema": DB_SCHEMA}')
    lines.append("")

    # Add primary key
    lines.append(
        "    id = mapped_column(Integer, primary_key=True, index=True, autoincrement=True)"
    )

    # Get base class fields to avoid duplication
    base_class_name = relationships.get(class_name, {}).get("base_class")
    base_fields = set()
    if base_class_name and base_class_name in dataclasses:
        base_fields = {f.name for f in fields(dataclasses[base_class_name])}

    # Get relationship info
    class_relationships = relationships.get(class_name, {})
    foreign_keys = class_relationships.get("foreign_keys", {})

    # Process fields
    for field in fields(dataclass_obj):
        field_name = field.name

        # Skip if configured or if it's a container
        if should_skip_field(field_name, class_name, relationships):
            continue

        # Skip foreign key references (handle separately)
        if field_name in foreign_keys or field_name in configured_fks:
            continue

        # Skip base class fields
        if field_name in base_fields:
            continue

        metadata = field.metadata or {}
        description = metadata.get("description", "")

        # Get column name
        column_name = get_column_name(class_name, field_name, suffix)

        try:
            sa_type, kwargs = python_type_to_sqlalchemy(field.type, field_name)

            # Check if this field should be unique
            if column_name in unique_fields or field_name in unique_fields:
                kwargs["unique"] = True

            kwargs_str = ", ".join(f"{k}={v}" for k, v in kwargs.items())
            if kwargs_str:
                column_def = f"Column({sa_type}, {kwargs_str})"
            else:
                column_def = f"Column({sa_type})"

            if description:
                lines.append(f"    # {description}")
            lines.append(f"    {column_name} = {column_def}")
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.warning("Error processing field %s in %s: %s", field_name, class_name, e)
            continue

    # Add additional fields from configuration
    for field_name, (sa_type, kwargs) in additional_fields.items():
        kwargs_str = ", ".join(f"{k}={v}" for k, v in kwargs.items())
        if kwargs_str:
            column_def = f"Column({sa_type}, {kwargs_str})"
        else:
            column_def = f"Column({sa_type})"
        lines.append(f"    {field_name} = {column_def}")

    # Add foreign key fields from configuration
    for fk_field, fk_reference in configured_fks.items():
        lines.append(f"    {fk_field} = mapped_column(ForeignKey({fk_reference}))")

    # Add foreign key fields from inferred relationships
    for fk_field, referenced_class in foreign_keys.items():
        # Skip if already in configured FKs
        if fk_field in configured_fks:
            continue
        ref_config = MODEL_CONFIG.get(referenced_class, {})
        ref_table_name = ref_config.get("table_name", referenced_class)
        lines.append(f"    {fk_field} = mapped_column(ForeignKey({ref_table_name}.id))")

    # Add standard helper method
    lines.append("")
    lines.append("    def columns_to_dict(self):")
    lines.append('        """')
    lines.append("        Return a dictionary representation of a row.")
    lines.append('        """')
    lines.append("        dict_ = {}")
    lines.append("        for key in self.__mapper__.c.keys():")
    lines.append("            dict_[key] = getattr(self, key)")
    lines.append("        return dict_")

    return "\n".join(lines)


def generate_custom_source_methods() -> str:
    """Generate custom methods for the Source model."""
    lines = []
    lines.append("")
    lines.append("    def to_json(self):")  # No longer needs session parameter
    lines.append('        """Serializes the source to a JSON dict."""')
    lines.append("        return {")
    lines.append('            "name": self.name,')
    lines.append('            "coords_J2000": (self.RAJ2000, self.DECJ2000),')
    lines.append('            "hpx": int(self.Heal_Pix_Position),')
    lines.append('            "i_pol": self.I_Pol,')
    lines.append('            "major_ax": self.Major_Ax,')
    lines.append('            "minor_ax": self.Minor_Ax,')
    lines.append('            "pos_ang": self.Pos_Ang,')
    lines.append('            "spec_idx": self.Spec_Idx,')
    lines.append('            "polarization": {')
    lines.append('                "q": self.Q_Pol,')
    lines.append('                "u": self.U_Pol,')
    lines.append('                "v": self.V_Pol,')
    lines.append('                "frac": self.Pol_Frac,')
    lines.append('                "ang": self.Pol_Ang,')
    lines.append("            },")
    lines.append("        }")
    return "\n".join(lines)


# Supporting models removed - new simplified datamodel only has SkySource


def determine_generation_order(dataclasses: dict, relationships: dict) -> list:
    """
    Determine the order in which models should be generated to satisfy dependencies.

    Args:
        dataclasses: Dictionary of dataclass names to class objects
        relationships: Dictionary of relationship information

    Returns:
        List of class names in dependency order
    """
    # Classes we want to generate as SQL models
    classes_to_generate = set()
    for class_name in dataclasses.keys():
        # Skip abstract base classes and pure container classes
        if class_name in ["GlobalSkyModel", "MeasurementBase"]:
            continue
        classes_to_generate.add(class_name)

    order = []
    remaining = classes_to_generate.copy()
    max_iterations = len(remaining) * 2
    iteration = 0

    while remaining and iteration < max_iterations:
        iteration += 1
        for class_name in list(remaining):
            # Get foreign key dependencies
            fks = relationships.get(class_name, {}).get("foreign_keys", {})

            # Check if all FK dependencies are satisfied
            dependencies_satisfied = True
            for _, referenced_class in fks.items():
                if referenced_class in remaining:
                    dependencies_satisfied = False
                    break

            if dependencies_satisfied:
                order.append(class_name)
                remaining.remove(class_name)

    # Add any remaining (circular dependencies)
    order.extend(remaining)

    return order


def generate_db_schema_file(  # pylint: disable=too-many-statements
    output_path: Path,
    module_name: str = "ska_sdp_datamodels.global_sky_model.global_sky_model",
) -> None:
    """
    Generate the complete db_schema.py file dynamically.

    Args:
        output_path: Path where the generated file should be written
        module_name: Fully qualified name of the module to introspect
    """
    logger.info("Generating database schema at %s", output_path)
    logger.info("Introspecting module: %s", module_name)

    # Discover dataclasses
    dataclasses = discover_dataclasses(module_name)
    logger.info("Discovered %d dataclasses: %s", len(dataclasses), list(dataclasses.keys()))

    # Infer relationships
    relationships = infer_relationships(dataclasses)

    # Determine generation order
    generation_order = determine_generation_order(dataclasses, relationships)
    logger.info("Generation order: %s", generation_order)

    lines = []

    # Get ska_sdp_datamodels version
    datamodels_version = get_package_version("ska-sdp-datamodels")

    # File header
    lines.append('"""')
    lines.append("Data models for SQLAlchemy")
    lines.append("")
    lines.append("This file is AUTO-GENERATED by scripts/generate_db_schema.py")
    lines.append(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"Source module: {module_name}")
    lines.append(f"ska-sdp-datamodels version: {datamodels_version}")
    lines.append("")
    lines.append("DO NOT EDIT THIS FILE MANUALLY!")
    lines.append("To regenerate, run: make generate-schema")
    lines.append('"""')
    lines.append("")
    lines.append("# pylint: disable=too-few-public-methods")
    lines.append("# pylint: disable=no-member")
    lines.append("")
    lines.append("import logging")
    lines.append("")
    lines.append(
        "from sqlalchemy import BigInteger, Boolean, Column, Float, Integer, String"
    )
    lines.append("from sqlalchemy.dialects.postgresql import JSON, TEXT")
    lines.append("from sqlalchemy.orm import mapped_column")
    lines.append("")
    lines.append("from ska_sdp_global_sky_model.configuration.config import DB_SCHEMA, Base")
    lines.append("")
    lines.append("logger = logging.getLogger(__name__)")
    lines.append("")
    lines.append("")

    # Generate models from dataclasses in dependency order
    for class_name in generation_order:
        if class_name not in dataclasses:
            continue

        dataclass_obj = dataclasses[class_name]
        model_code = generate_model_from_dataclass(
            class_name, dataclass_obj, relationships, dataclasses
        )

        # Add custom methods if configured
        config = MODEL_CONFIG.get(class_name, {})
        if config.get("custom_methods"):
            model_code += generate_custom_source_methods()

        lines.append(model_code)
        lines.append("")
        lines.append("")

    # Write to file
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    logger.info("Successfully generated %s", output_path)


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Generate SQLAlchemy database schema from datamodels"
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=Path(__file__).parent.parent
        / "src"
        / "ska_sdp_global_sky_model"
        / "api"
        / "app"
        / "db_schema.py",
        help="Output path for generated db_schema.py file",
    )
    parser.add_argument(
        "--module",
        "-m",
        type=str,
        default="ska_sdp_datamodels.global_sky_model.global_sky_model",
        help="Module name to introspect for dataclasses",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        generate_db_schema_file(args.output, args.module)
        logger.info("Schema generation completed successfully!")
        return 0
    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error("Schema generation failed: %s", e, exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
