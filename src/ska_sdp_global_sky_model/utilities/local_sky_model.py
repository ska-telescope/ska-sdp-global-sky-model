"""
Provides save and load functions for a local sky model.
"""

from __future__ import annotations

import logging
import math
import os
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar, Literal, Union, get_args, get_origin

import numpy
from ska_sdp_datamodels.global_sky_model.global_sky_model import SkyComponent
from ska_sdp_dataproduct_metadata import MetaData

LOGGER = logging.getLogger(__name__)


def _get_str_columns() -> set[str]:
    """
    Determine string column names from SkyComponent dataclass.

    Returns:
        Set of column names that should be treated as strings
    """
    str_cols = set()
    for field_name, field_type in SkyComponent.__annotations__.items():
        # Handle Optional types (Union[type, None])
        origin = get_origin(field_type)
        if origin is Union:
            args = get_args(field_type)
            if args and len(args) >= 1 and args[0] is str:
                str_cols.add(field_name)
        elif field_type is str:
            str_cols.add(field_name)

    # Ensure known string columns are always included
    str_cols.update({"component_id", "name"})
    return str_cols


def _get_bool_columns() -> set[str]:
    """
    Determine boolean column names from SkyComponent dataclass.

    Returns:
        Set of column names that should be treated as booleans
    """
    bool_cols = set()
    for field_name, field_type in SkyComponent.__annotations__.items():
        # Handle Optional types (Union[type, None])
        origin = get_origin(field_type)
        if origin is Union:
            args = get_args(field_type)
            if args and len(args) >= 1 and args[0] is bool:
                bool_cols.add(field_name)
        elif field_type is bool:
            bool_cols.add(field_name)

    # Include alternate names for backwards compatibility
    bool_cols.add("logarithmicsi")
    return bool_cols


def _get_vector_float_columns() -> set[str]:
    """
    Determine vector float column names from SkyComponent dataclass.

    Returns:
        Set of column names that should be treated as vector floats
    """
    vector_cols = set()
    for field_name, field_type in SkyComponent.__annotations__.items():
        # Handle Optional types (Union[type, None])
        origin = get_origin(field_type)

        # Check for list types
        if origin is list:
            vector_cols.add(field_name)
        elif origin is Union:
            args = get_args(field_type)
            if args and len(args) >= 1:
                inner_origin = get_origin(args[0])
                if inner_origin is list:
                    vector_cols.add(field_name)

    # Include alternate names for backwards compatibility
    vector_cols.add("spectralindex")
    return vector_cols


@dataclass
class LocalSkyModel:
    """
    Sky model data class as a structure of numpy.ndarray arrays.
    """

    columns: list[str]
    num_rows: int
    max_vector_len: int

    # --------------------------
    # Static configuration.
    # --------------------------

    # Column type enumerator.
    column_type = Literal["float", "str", "int", "bool", "vector_float"]

    # Names of non-float column types (anything else is treated as a float):
    # Dynamically determined from SkyComponent dataclass if available
    _STR_COLUMNS: ClassVar[set] = _get_str_columns()
    _INT_COLUMNS: ClassVar[set] = {}
    _BOOL_COLUMNS: ClassVar[set] = _get_bool_columns()

    # Names of default-vector-float columns:
    # Dynamically determined from SkyComponent dataclass if available
    _VECTOR_FLOAT_COLUMNS: ClassVar[set] = _get_vector_float_columns()
    _NUM_TERMS: ClassVar[str] = "_num_terms"  # Key suffix for vector length.

    # Sentinel values for null "missing" entries. (Using NaN for floats.)
    INT_MISSING: ClassVar[numpy.int32] = numpy.iinfo(numpy.int32).min
    BOOL_MISSING: ClassVar[numpy.int8] = numpy.int8(-1)

    # --------------------------
    # Internal schema and storage.
    # --------------------------

    @dataclass(frozen=True)
    class ColumnSpec:
        """
        Column specifier: contains column name and enumerated data type.
        """

        name: str
        column_type: "LocalSkyModel.column_type"

    # Schema is a dictionary of column specifiers.
    schema: dict[str, "LocalSkyModel.ColumnSpec"] = field(default_factory=dict)

    # Header and metadata key, value pairs.
    _header: dict[str, Any] = field(default_factory=dict)
    _metadata: dict[str, Any] = field(default_factory=dict)

    # Data are stored here.
    _cols: dict[str, numpy.ndarray | list[str]] = field(default_factory=dict)

    # --------------------------
    # Column wrapper.
    # --------------------------

    @dataclass(frozen=True)
    class VectorColumn:
        """
        Helper for accessing columns containing vectors (i.e. spectral index).
        """

        values: numpy.ndarray
        num_terms: numpy.ndarray
        max_len: int

        def row(self, row_index: int) -> numpy.ndarray:
            """
            Returns the vector in a row of the column.

            :param row_index: Row index.
            :type row_index: int
            :return: The vector in the row.
            :rtype: ndarray
            """
            num_terms = int(self.num_terms[row_index])
            return self.values[row_index, :num_terms]

        def __getitem__(self, row_index: int) -> numpy.ndarray:
            """
            Convenience method that calls row().

            :param row_index: Row index.
            :type row_index: int
            :return: The vector in the row.
            :rtype: ndarray
            """
            return self.row(row_index)

    # --------------------------
    # Private static helpers.
    # --------------------------

    @staticmethod
    def _convert_to_float_array(value: Any, max_len: int) -> tuple[numpy.ndarray, int]:
        """
        Converts input to a numpy float array.

        :param value: Input (can be bracketed CSV string, or a list of values).
        :type value: Any
        :param max_len: Length of vector to return as a numpy array.
        :type max_len: int
        :return: Tuple containing the array, and the number of valid entries.
        :rtype: tuple[ndarray, int]
        """
        out = numpy.full((max_len,), numpy.nan, dtype=numpy.float64)
        if value is None:
            return out, 0

        # If the input is already an iterable list, just use it.
        if isinstance(value, (list, tuple, numpy.ndarray)):
            seq = list(value)
            num_elements = min(len(seq), max_len)
            for i in range(num_elements):
                out[i] = float(seq[i])
            return out, num_elements

        # Otherwise, if not a list, process a string.
        raw = str(value).strip()
        if raw in ("", "[]"):
            return out, 0  # Empty string, or empty list.
        if raw[0] == "[" and raw[-1] == "]":
            raw = raw[1:-1]  # Strip brackets.
        parts = [p.strip() for p in raw.split(",") if p.strip()]
        num_elements = min(len(parts), max_len)
        for i in range(num_elements):
            try:
                out[i] = float(parts[i])
            except ValueError:
                out[i] = numpy.nan
        return out, num_elements

    @staticmethod
    def _find_format_string(lines: Sequence[str]) -> str | None:
        """
        Returns the core part (the column specifiers) of the format string.
        This removes outer brackets and the "format" identifier.

        :param lines: List of strings to check for presence of "format".
        :type lines: Sequence[str]
        :return: A string containing column specifiers in the format string.
        :rtype: str | None
        """
        for line in lines:
            stripped = line.strip()
            lower_case_line = stripped.lower()
            if stripped.startswith("#") and "format" in lower_case_line:
                after_hash = stripped[1:]
                left_bracket = after_hash.find("(")
                right_bracket = after_hash.rfind(")")
                if 0 <= left_bracket < right_bracket:
                    start = left_bracket + 1
                    return after_hash[start:right_bracket].strip()
        return None

    @staticmethod
    def _format_vector(vec_row: numpy.ndarray, num_terms: int) -> str:
        """
        Returns a string containing a row-vector, enclosed in square brackets.

        :param vec_row: Input vector, as an array.
        :type vec_row: numpy.ndarray
        :param num_terms: Number of elements to use from the vector.
        :type num_terms: int
        :return: Formatted string.
        :rtype: str
        """
        if num_terms <= 0:
            return "[]"
        if num_terms == 1:
            return str(vec_row[0])
        parts: list[str] = []
        for i in range(num_terms):
            value = float(vec_row[i])
            if math.isnan(value):
                break
            parts.append(f"{value:g}")
        return "[]" if not parts else "[" + ",".join(parts) + "]"

    @staticmethod
    def _get_column_type(
        name: str, vector_columns_lower: list[str]
    ) -> "LocalSkyModel.column_type":
        """
        Returns the column type for the given column name.

        :param name: Name of column.
        :type name: str
        :param vector_columns_lower: Names of columns containing vectors.
        :type vector_columns_lower: list[str]
        :return: Column type of specified column, as a string.
        :rtype: column_type
        """
        name_lower = name.lower()
        if name_lower in vector_columns_lower:
            return "vector_float"
        if name_lower in LocalSkyModel._BOOL_COLUMNS:
            return "bool"
        if name_lower in LocalSkyModel._INT_COLUMNS:
            return "int"
        if name_lower in LocalSkyModel._STR_COLUMNS:
            return "str"
        return "float"

    @staticmethod
    def _handle_quote(
        character: str, in_quotes: bool, quote_char: str | None
    ) -> tuple[bool, str | None]:
        """Handle quote character and return updated quote state."""
        if not in_quotes:
            return True, character
        if character == quote_char:
            return False, None
        return in_quotes, quote_char

    @staticmethod
    def _handle_bracket(character: str, bracket_depth: int) -> tuple[bool, int]:
        """Handle bracket character. Returns (was_handled, new_depth)."""
        if character == "[":
            return True, bracket_depth + 1
        if character == "]" and bracket_depth > 0:
            return True, bracket_depth - 1
        return False, bracket_depth

    @staticmethod
    def _is_separator(character: str, in_quotes: bool, bracket_depth: int) -> bool:
        """Check if character is a token separator."""
        return character == "," and not in_quotes and bracket_depth == 0

    @staticmethod
    def _tokenize_line(line: str) -> list[str]:
        """
        Split a line into tokens, assuming commas as separators,
        while respecting quotes and bracketed vectors.

        :param line: String to split up.
        :type line: str
        :return: List of tokens.
        :rtype: list[str]
        """
        if line is None:
            return []

        tokens: list[str] = []
        buf: list[str] = []
        bracket_depth = 0
        in_quotes = False
        quote_char: str | None = None

        for character in line:
            # Handle quotes
            if character in ("'", '"'):
                in_quotes, quote_char = LocalSkyModel._handle_quote(
                    character, in_quotes, quote_char
                )
                buf.append(character)
                continue

            # Handle brackets when not in quotes
            if not in_quotes:
                handled, bracket_depth = LocalSkyModel._handle_bracket(character, bracket_depth)
                if handled:
                    buf.append(character)
                    continue

                # Handle separator
                if LocalSkyModel._is_separator(character, in_quotes, bracket_depth):
                    tokens.append("".join(buf).strip())
                    buf = []
                    continue

            buf.append(character)

        # Finalize last token
        if buf:
            tokens.append("".join(buf).strip())
        elif line and line[-1] == ",":
            tokens.append("")

        return tokens

    # --------------------------
    # Constructor.
    # --------------------------

    @classmethod
    def empty(
        cls,
        column_names: list[str],
        num_rows: int,
        max_vector_len: int = 5,
        vector_columns: Sequence[str] | None = None,
    ) -> "LocalSkyModel":
        """
        Create a sized, empty sky model containing the specified columns.

        :param column_names: List of column names.
        :type column_names: list[str]
        :param num_rows: Number of empty rows to create.
        :type num_rows: int
        :param max_vector_len: Maximum length of vector in vector columns.
        :type max_vector_len: int
        :param vector_columns: Names of columns containing vectors.
        :type vector_columns: Sequence[str] or None
        :return: An empty sky model.
        :rtype: LocalSkyModel
        """
        # Define the columns that could be vectors (use defaults if not given).
        vector_columns = (
            list(vector_columns) if vector_columns is not None else list(cls._VECTOR_FLOAT_COLUMNS)
        )
        vector_columns_lower = [column.lower() for column in vector_columns]

        # Construct the schema based on the supplied columns.
        schema: dict[str, LocalSkyModel.ColumnSpec] = {}
        for column in column_names:
            schema[column] = cls.ColumnSpec(
                column, cls._get_column_type(column, vector_columns_lower)
            )

        # Create empty dictionary for column data.
        # The dictionary key is the column name.
        cols: dict[str, numpy.ndarray | list[str]] = {}

        # Loop over each item in the schema and create the appropriate array.
        # Fill each array with the "missing" sentinel value.
        for name, spec in schema.items():
            if spec.column_type == "float":
                cols[name] = numpy.full((num_rows,), numpy.nan, dtype=numpy.float64)
            elif spec.column_type == "int":
                cols[name] = numpy.full((num_rows,), cls.INT_MISSING, dtype=numpy.int32)
            elif spec.column_type == "bool":
                cols[name] = numpy.full((num_rows,), cls.BOOL_MISSING, dtype=numpy.int8)
            elif spec.column_type == "str":
                cols[name] = [""] * num_rows
            elif spec.column_type == "vector_float":
                cols[name] = numpy.full((num_rows, max_vector_len), numpy.nan, dtype=numpy.float64)
                cols[name + cls._NUM_TERMS] = numpy.zeros((num_rows,), dtype=numpy.uint8)
            else:
                raise ValueError(spec.column_type)

        # Return the constructed data model.
        return cls(
            columns=column_names,
            num_rows=num_rows,
            max_vector_len=max_vector_len,
            schema=schema,
            _cols=cols,
        )

    # --------------------------
    # Public I/O methods.
    # --------------------------

    @classmethod
    def load(
        cls,
        path: str,
        max_vector_len: int = 5,
        vector_columns: Sequence[str] | None = None,
    ) -> "LocalSkyModel":
        """
        Load a sky model CSV text file into the data model.

        :param path: Path to CSV text file to load.
        :type path: str
        :param max_vector_len: Maximum vector length for vector column types.
        :type max_vector_len: int
        :param vector_columns: Names of columns that may contain vectors.
        :type vector_columns: Sequence[str] or None
        :return: Sky model data structure.
        :rtype: LocalSkyModel
        """

        # Get the columns that may contain vectors.
        vector_columns = (
            list(vector_columns) if vector_columns is not None else list(cls._VECTOR_FLOAT_COLUMNS)
        )

        # Load the file and split lines into a list of strings.
        lines = Path(path).read_text("utf-8").splitlines()

        # Find the format string and split it into a list of column names.
        columns = cls._tokenize_line(cls._find_format_string(lines))

        # Check that columns are defined.
        if not columns:
            raise RuntimeError("Columns are not defined: Check the format string")

        # Count the number of rows, and create an empty sky model.
        num_rows = sum(1 for line in lines if not line.lstrip().startswith("#"))
        model = cls.empty(
            column_names=columns,
            num_rows=num_rows,
            max_vector_len=max_vector_len,
            vector_columns=vector_columns,
        )

        # Loop over lines.
        row = 0
        for line in lines:
            # Ignore if not a data line.
            if line.lstrip().startswith("#"):
                continue

            # Split up the line.
            tokens = cls._tokenize_line(line.strip())

            # Loop over columns in the line, storing each value.
            for column_index, name in enumerate(columns):
                model.set_value(name, row, tokens[column_index])

            # Next line. Increment row counter for the next valid line.
            row += 1

        return model

    def _format_float_value(self, name: str, row_index: int) -> str:
        """Format a float column value."""
        value = float(self._cols[name][row_index])
        return "" if math.isnan(value) else f"{value:g}"

    def _format_int_value(self, name: str, row_index: int) -> str:
        """Format an int column value."""
        value = int(self._cols[name][row_index])
        return "" if value == self.INT_MISSING else str(value)

    def _format_bool_value(self, name: str, row_index: int) -> str:
        """Format a bool column value."""
        value = int(self._cols[name][row_index])
        if value == int(self.BOOL_MISSING):
            return ""
        return "true" if value == 1 else "false"

    def _format_vector_column_value(self, name: str, row_index: int) -> str:
        """Format a vector_float column value."""
        return self._format_vector(
            self._cols[name][row_index],
            self._cols[name + self._NUM_TERMS][row_index],
        )

    def _format_column_value(self, name: str, row_index: int) -> str:
        """
        Format a single column value as a string token for CSV output.

        :param name: Column name.
        :type name: str
        :param row_index: Row index.
        :type row_index: int
        :return: Formatted string token.
        :rtype: str
        """
        column_type = self.schema[name].column_type

        formatters = {
            "float": self._format_float_value,
            "int": self._format_int_value,
            "bool": self._format_bool_value,
            "str": lambda n, r: self._cols[n][r],
            "vector_float": self._format_vector_column_value,
        }

        formatter = formatters.get(column_type)
        return formatter(name, row_index) if formatter else ""

    def _write_header(self, out) -> None:
        """Write format string and header to file."""
        format_string = ",".join(self.columns)
        out.write(f"# ({format_string}) = format\n")
        out.write(f"# NUMBER_OF_COMPONENTS: {self.num_rows}\n")
        for key, value in self._header.items():
            out.write(f"# {key}={str(value)}\n")

    def _write_row(self, out, row_index: int) -> None:
        """Write a single data row to file."""
        tokens = [self._format_column_value(name, row_index) for name in self.columns]
        out.write(",".join(tokens) + "\n")

    def save(self, path: str, metadata_dir: str = ".") -> None:
        """
        Save this sky model to a CSV text file.

        :param path: Path of CSV text file to write.
        :type path: str
        :param metadata_dir: Directory in which to write YAML metadata file.
        :type metadata_dir: str
        """
        with open(path, "w", encoding="utf-8") as out:
            self._write_header(out)
            for row_index in range(self.num_rows):
                self._write_row(out, row_index)

        if metadata_dir:
            self.save_metadata(os.path.join(metadata_dir, "ska-data-product.yaml"), path)

    def save_metadata(self, yaml_path: str, lsm_path: str) -> None:
        """
        Saves the metadata for this sky model to a YAML file.
        This is called by save(), so it should not normally be called
        separately.

        :param yaml_path: Path to YAML file to write.
        :type yaml_path: str
        :param lsm_path: Path of local sky model file.
        :type lsm_path: str
        """
        metadata = MetaData()
        metadata.output_path = yaml_path

        # Write any special values that have been set
        # (e.g. the execution block ID).
        if "execution_block_id" in self._metadata:
            metadata.set_execution_block_id(self._metadata["execution_block_id"])

        # Get a handle to the top-level metadata dictionary.
        data = metadata.get_data()

        # Create the header dictionary.
        header = {
            "NUMBER_OF_COMPONENTS": self.num_rows,
        }
        header.update(self._header)

        # Update the metadata contents.
        parent = "local_sky_model"
        data[parent] = {}
        data[parent]["header"] = header
        data[parent]["columns"] = self.column_names

        # Save the LSM file name in the metadata.
        metadata.new_file(
            dp_path=lsm_path,
            description="Local sky model CSV text file",
        )

        # Write to disk (automatically validates the metadata).
        try:
            metadata.write()
        except MetaData.ValidationError as err:
            LOGGER.error("Validation failed with error(s): %s", err.errors)
            raise err

    # --------------------------
    # Other public accessors.
    # --------------------------

    def __getitem__(self, column_name: str):
        """
        Returns a reference to the specified column.

        :param column_name: Column name.
        :type column_name: str
        """
        column_type = self.schema[column_name].column_type
        if column_type in ("float", "int", "bool", "str"):
            return self._cols[column_name]
        if column_type == "vector_float":
            return self.VectorColumn(
                values=self._cols[column_name],
                num_terms=self._cols[column_name + self._NUM_TERMS],
                max_len=self.max_vector_len,
            )
        raise KeyError(column_name)

    @property
    def column_names(self) -> list[str]:
        """
        Return a list of column names in the sky model.

        :return: List of column names.
        :rtype: list[str]
        """
        return self.columns

    def set_header(self, header: dict[str, Any]) -> None:
        """
        Set header key, value pairs. These are written to the file as comments.

        :param header: Header data to set.
        :type header: dict[str, Any]
        """
        for key, value in header.items():
            self._header[key] = value

    def set_metadata(self, metadata: dict[str, Any]) -> None:
        """
        Set metadata key, value pairs.
        These are written to appropriate sections of the YAML file.

        :param metadata: Metadata values to set.
        :type metadata: dict[str, Any]
        """
        for key, value in metadata.items():
            self._metadata[key] = value

    def set_row(self, row_index: int, row_data: dict[str, Any]) -> None:
        """
        Sets all parameters for a single component.

        :param row_index: Row index of component to set.
        :type row_index: int
        :param row_data: Data to set for this row.
        :type row_data: dict[str, Any]
        """
        for name, value in row_data.items():
            if name not in self.schema:
                raise KeyError(f"Unknown column name: {name}")
            self.set_value(name, row_index, value)

    def _set_float_value(self, name: str, row_index: int, value: Any) -> None:
        """Set a float column value."""
        try:
            self._cols[name][row_index] = float(value)
        except (TypeError, ValueError):
            self._cols[name][row_index] = numpy.nan

    def _set_int_value(self, name: str, row_index: int, value: Any) -> None:
        """Set an int column value."""
        try:
            self._cols[name][row_index] = int(value)
        except (TypeError, ValueError):
            self._cols[name][row_index] = self.INT_MISSING

    def _set_bool_value(self, name: str, row_index: int, value: Any) -> None:
        """Set a bool column value."""
        if value is None:
            self._cols[name][row_index] = self.BOOL_MISSING
        else:
            val = str(value).strip().lower()
            self._cols[name][row_index] = numpy.int8(1 if val in ("true", "t", "1") else 0)

    def _set_str_value(self, name: str, row_index: int, value: Any) -> None:
        """Set a string column value."""
        self._cols[name][row_index] = "" if value is None else str(value)

    def _set_vector_value(self, name: str, row_index: int, value: Any) -> None:
        """Set a vector_float column value."""
        self._cols[name][row_index, :], self._cols[name + self._NUM_TERMS][row_index] = (
            self._convert_to_float_array(value, self.max_vector_len)
        )

    def _normalize_value(self, value: Any) -> Any:
        """Normalize value by treating empty strings as None."""
        if isinstance(value, str):
            value = value.strip()
            if value == "":
                return None
        return value

    def set_value(self, name: str, row_index: int, value: Any) -> None:
        """
        Sets a single parameter for a single component.

        :param name: Column name to set.
        :type name: str
        :param row_index: Row index of component to set.
        :type row_index: int
        :param value: Value to set for parameter. Use 'None' for 'missing'.
        :type value: Any
        """
        value = self._normalize_value(value)
        column_type = self.schema[name].column_type

        if column_type == "float":
            self._set_float_value(name, row_index, value)
        elif column_type == "int":
            self._set_int_value(name, row_index, value)
        elif column_type == "bool":
            self._set_bool_value(name, row_index, value)
        elif column_type == "str":
            self._set_str_value(name, row_index, value)
        elif column_type == "vector_float":
            self._set_vector_value(name, row_index, value)
        else:
            raise ValueError(f"Unknown column type: {column_type}")
