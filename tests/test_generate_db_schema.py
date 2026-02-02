"""
Tests for the generate_db_schema.py script.

This module tests the schema generation functionality to ensure
that dataclasses are correctly converted to SQLAlchemy models.
"""

# Import the generator functions
import sys
import tempfile
from pathlib import Path
from typing import List

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

try:
    from generate_db_schema import (
        generate_db_schema_file,
        python_type_to_sqlalchemy,
    )
except ImportError:
    pytest.skip("generate_db_schema.py not available", allow_module_level=True)


class TestPythonTypeToSQLAlchemy:
    """Tests for type conversion from Python to SQLAlchemy."""

    def test_convert_int(self):
        """Test conversion of int type."""
        sa_type, kwargs = python_type_to_sqlalchemy(int, "test_field")
        assert sa_type == "Integer"
        assert kwargs == {}

    def test_convert_float(self):
        """Test conversion of float type."""
        sa_type, kwargs = python_type_to_sqlalchemy(float, "test_field")
        assert sa_type == "Float"
        assert kwargs == {}

    def test_convert_str(self):
        """Test conversion of str type."""
        sa_type, kwargs = python_type_to_sqlalchemy(str, "test_field")
        assert sa_type == "String"
        assert kwargs == {}

    def test_convert_bool(self):
        """Test conversion of bool type."""
        sa_type, kwargs = python_type_to_sqlalchemy(bool, "test_field")
        assert sa_type == "Boolean"
        assert kwargs == {}

    def test_convert_optional_int(self):
        """Test conversion of optional int type (int | None)."""
        sa_type, kwargs = python_type_to_sqlalchemy(int | None, "test_field")
        assert sa_type == "Integer"
        assert kwargs == {"nullable": True}

    def test_convert_optional_float(self):
        """Test conversion of optional float type (float | None)."""
        sa_type, kwargs = python_type_to_sqlalchemy(float | None, "test_field")
        assert sa_type == "Float"
        assert kwargs == {"nullable": True}

    def test_convert_optional_str(self):
        """Test conversion of optional str type (str | None)."""
        sa_type, kwargs = python_type_to_sqlalchemy(str | None, "test_field")
        assert sa_type == "String"
        assert kwargs == {"nullable": True}

    def test_convert_list(self):
        """Test conversion of List type to JSON."""
        sa_type, kwargs = python_type_to_sqlalchemy(List[float], "test_field")
        assert sa_type == "JSON"
        assert kwargs == {"nullable": True}


class TestGenerateDBSchemaFile:
    """Tests for the complete schema generation."""

    def test_generate_schema_file_creates_file(self):
        """Test that the schema generation creates a file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test_db_schema.py"
            generate_db_schema_file(output_path)

            assert output_path.exists()
            assert output_path.is_file()

    def test_generated_schema_has_required_imports(self):
        """Test that generated schema contains necessary imports."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test_db_schema.py"
            generate_db_schema_file(output_path)

            content = output_path.read_text()

            # Check for essential imports
            assert "from sqlalchemy import" in content
            assert "from sqlalchemy.orm import" in content
            assert "import logging" in content
            assert "from ska_sdp_global_sky_model.configuration.config import" in content

    def test_generated_schema_has_all_models(self):
        """Test that generated schema contains all required models."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test_db_schema.py"
            generate_db_schema_file(output_path)

            content = output_path.read_text()

            # Check for Source model
            assert "class Source(Base):" in content
            # Should not have old measurement tables (no stubs)
            assert "DEPRECATED" not in content

    def test_generated_schema_has_source_methods(self):
        """Test that Source model has the required methods."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test_db_schema.py"
            generate_db_schema_file(output_path)

            content = output_path.read_text()

            # Check for custom methods in Source class
            assert "def to_json(self):" in content
            assert "def columns_to_dict(self):" in content

    def test_generated_schema_has_columns_to_dict_methods(self):
        """Test that all models have columns_to_dict method."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test_db_schema.py"
            generate_db_schema_file(output_path)

            content = output_path.read_text()

            # Check that columns_to_dict appears (once for Source model)
            assert "def columns_to_dict(self):" in content

    def test_generated_schema_has_source_fields(self):
        """Test that generated schema includes expected Source fields."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test_db_schema.py"
            generate_db_schema_file(output_path)

            content = output_path.read_text()

            # Check for key fields in Source model
            assert "ra = Column(Float)" in content
            assert "dec = Column(Float)" in content
            assert "i_pol = Column(Float)" in content
            assert "spec_idx = Column(JSON" in content
            assert "healpix_index = Column(BigInteger" in content

    def test_generated_schema_has_primary_keys(self):
        """Test that all models have primary key definitions."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test_db_schema.py"
            generate_db_schema_file(output_path)

            content = output_path.read_text()

            # Check for primary key columns (Source model)
            assert "primary_key=True" in content

    def test_generated_schema_has_table_args(self):
        """Test that models include schema configuration."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test_db_schema.py"
            generate_db_schema_file(output_path)

            content = output_path.read_text()

            # Check for schema configuration
            assert '__table_args__ = {"schema": DB_SCHEMA}' in content
            # Should appear once per model
            assert content.count('__table_args__ = {"schema": DB_SCHEMA}') >= 1

    def test_generated_schema_is_valid_python(self):
        """Test that the generated schema is valid Python code."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test_db_schema.py"
            generate_db_schema_file(output_path)

            content = output_path.read_text()

            # Try to compile the code to check for syntax errors
            try:
                compile(content, str(output_path), "exec")
            except SyntaxError as e:
                pytest.fail(f"Generated schema has syntax errors: {e}")

    def test_generated_schema_has_auto_generated_warning(self):
        """Test that generated file includes warning about auto-generation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test_db_schema.py"
            generate_db_schema_file(output_path)

            content = output_path.read_text()

            # Check for warning message
            assert "AUTO-GENERATED" in content
            assert "DO NOT EDIT THIS FILE MANUALLY" in content
            assert "make generate-schema" in content

    def test_generated_schema_has_source_position_fields(self):
        """Test that Source model has position-related fields."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test_db_schema.py"
            generate_db_schema_file(output_path)

            content = output_path.read_text()

            # Check for position fields in Source model
            assert "ra = Column(Float)" in content
            assert "dec = Column(Float)" in content
            assert "healpix_index" in content
