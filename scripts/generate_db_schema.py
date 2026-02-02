#!/usr/bin/env python3
"""
Generate SQLAlchemy database schema from ska_sdp_datamodels dataclasses.

This script dynamically generates the db_schema.py file by introspecting
the dataclasses defined in ska_sdp_datamodels.global_sky_model.global_sky_model
and mapping them to appropriate SQLAlchemy models with correct column types.

The script is fully dynamic and will adapt to any changes in the datamodel
structure. Database-specific configurations are maintained in db_config.py.
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
sys.path.insert(0, str(Path(__file__).parent))

# Import database configuration
from db_config import (
    get_additional_fields,
    get_column_name,
    get_json_mapping,
    get_table_name,
    get_unique_fields,
    has_custom_json,
    should_skip_model,
)

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
    # Skip container fields (lists, dicts of other dataclasses)
    if field_name in relationships.get(class_name, {}).get("containers", {}):
        return True

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
    # Use db_config for all configuration
    table_name = get_table_name(class_name)
    additional_fields = get_additional_fields(class_name)
    unique_fields = get_unique_fields(class_name)

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

        # Skip if it's a container
        if should_skip_field(field_name, class_name, relationships):
            continue

        # Skip foreign key references (handle separately)
        if field_name in foreign_keys:
            continue

        # Skip base class fields
        if field_name in base_fields:
            continue

        metadata = field.metadata or {}
        description = metadata.get("description", "")

        # Get column name using db_config
        column_name = get_column_name(class_name, field_name)

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

    # Add additional fields from db_config
    for field_name, field_config in additional_fields.items():
        sa_type = field_config["type"]
        kwargs = field_config["kwargs"]
        field_desc = field_config.get("description", "")

        kwargs_str = ", ".join(f"{k}={v}" for k, v in kwargs.items())
        if kwargs_str:
            column_def = f"Column({sa_type}, {kwargs_str})"
        else:
            column_def = f"Column({sa_type})"

        if field_desc:
            lines.append(f"    # {field_desc}")
        lines.append(f"    {field_name} = {column_def}")

    # Add foreign key fields from inferred relationships
    for fk_field, referenced_class in foreign_keys.items():
        ref_table_name = get_table_name(referenced_class)
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

    # Add custom JSON serialization if configured
    if has_custom_json(class_name):
        lines.append("")
        lines.append(generate_to_json_method(class_name))

    return "\n".join(lines)


def generate_to_json_method(class_name: str) -> str:
    """
    Generate a custom to_json() method dynamically from configuration.

    Args:
        class_name: Name of the dataclass

    Returns:
        String containing the to_json method definition
    """
    json_mapping = get_json_mapping(class_name)

    lines = []
    lines.append("    def to_json(self):")
    lines.append('        """Serializes the instance to a JSON dict."""')
    lines.append("        return {")

    for json_key, db_field in json_mapping.items():
        if isinstance(db_field, tuple):
            # Tuple means we're combining multiple fields (e.g., coordinates)
            field_refs = ", ".join(f"self.{f}" for f in db_field)
            lines.append(f'            "{json_key}": ({field_refs}),')
        elif isinstance(db_field, dict):
            # Nested dictionary
            lines.append(f'            "{json_key}": {{')
            for nested_key, nested_field in db_field.items():
                lines.append(f'                "{nested_key}": self.{nested_field},')
            lines.append("            },")
        else:
            # Simple field mapping
            # Handle special case for int conversion of healpix_index
            if json_key == "healpix_index":
                lines.append(f'            "{json_key}": int(self.{db_field}),')
            else:
                lines.append(f'            "{json_key}": self.{db_field},')

    lines.append("        }")

    return "\n".join(lines)


def determine_generation_order(dataclasses: dict, relationships: dict) -> list:
    """
    Determine the order in which models should be generated to satisfy dependencies.

    Args:
        dataclasses: Dictionary of dataclass names to class objects
        relationships: Dictionary of relationship information

    Returns:
        List of class names in dependency order
    """
    # Classes we want to generate as SQL models (skip those in db_config)
    classes_to_generate = set()
    for class_name in dataclasses.keys():
        if not should_skip_model(class_name):
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
    lines.append("Database-specific configuration in: scripts/db_config.py")
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
