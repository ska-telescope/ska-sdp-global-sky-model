"""
This module contains tests for local_sky_model.py
"""

# pylint: disable=protected-access

import os
import tempfile

import numpy
import pytest
import yaml

from ska_sdp_global_sky_model.utilities.local_sky_model import LocalSkyModel


class TestLocalSkyModel:
    """
    Tests for the LocalSkyModel class.
    """

    def test_set_values(self):
        """
        Test that we can set some values in the data structure.
        """
        # Define the columns we can store using the list of names.
        column_names = [
            "component_id",
            "ra",
            "dec",
            "i_pol",
            "ref_freq",
            "spec_idx",
            "log_spec_idx",
            "q_pol",
            "u_pol",
            "v_pol",
        ]
        num_rows = 100
        model = LocalSkyModel.empty(
            column_names=column_names,
            num_rows=num_rows,
        )

        # Check that the columns match.
        assert column_names == model.column_names

        # Set some values in the model based on a pattern.
        for i in range(num_rows):
            model.set_row(
                i,
                {
                    "component_id": f"GLEAM J{10 * i:06d}-{i:06d}",
                    "ra": 11.0 * i,
                    "dec": -i,
                    "i_pol": 10.0 * i,
                    "spec_idx": [-0.7, i / 100.0, 0.123],
                    "log_spec_idx": bool(i % 2),
                    "ref_freq": 100e6 + 1e6 * i,
                    "q_pol": 2.0 * i,
                    "u_pol": -1.1 * i,
                    "v_pol": 0.1 * i,
                },
            )

        # Override some values.
        model.set_value("q_pol", 3, 300)
        model.set_value("ref_freq", 2, 300e6)
        model.set_value("spec_idx", 1, [0.987, 0.654])

        # Assert that an error is raised when trying to set an unknown field.
        with pytest.raises(KeyError):
            model.set_value("an_unknown_column", 0, 345)

        # Check that the values were set correctly.
        for i in range(num_rows):
            assert model["component_id"][i] == f"GLEAM J{10 * i:06d}-{i:06d}"
            assert model["ra"][i] == 11.0 * i
            assert model["dec"][i] == -i
            assert model["i_pol"][i] == 10.0 * i
            assert bool(model["log_spec_idx"][i]) == bool(i % 2)
            if i == 1:
                # Overridden value.
                assert model["spec_idx"].num_terms[i] == 2
                assert numpy.allclose(model["spec_idx"][i], [0.987, 0.654])
            else:
                assert model["spec_idx"].num_terms[i] == 3
                assert numpy.allclose(model["spec_idx"][i], [-0.7, i / 100.0, 0.123])
            if i == 2:
                # Overridden value.
                assert model["ref_freq"][i] == 300e6
            else:
                assert model["ref_freq"][i] == 100e6 + 1e6 * i
            if i == 3:
                # Overridden value.
                assert model["q_pol"][i] == 300
            else:
                assert model["q_pol"][i] == 2.0 * i
            assert model["u_pol"][i] == -1.1 * i
            assert model["v_pol"][i] == 0.1 * i

    # pylint: disable=too-many-locals
    def test_save_and_load(self):
        """
        Test that we can save values to a CSV file and load them back.
        """
        # Define the columns we can store using the list of names.
        column_names = [
            "component_id",
            "ra",
            "dec",
            "i_pol",
            "ref_freq",
            "spec_idx",
            "log_spec_idx",
            "q_pol",
            "u_pol",
            "v_pol",
        ]
        num_rows = 20000
        model = LocalSkyModel.empty(
            column_names=column_names,
            num_rows=num_rows,
        )

        # Set some values in the model based on a pattern.
        for i in range(num_rows):
            model.set_row(
                i,
                {
                    "component_id": f"GLEAM J{10 * i:06d}-{i:06d}",
                    "ra": 11.0 * i,
                    "dec": -i,
                    "i_pol": 10.0 * i,
                    "spec_idx": [-0.7, i / 100.0, 0.123],
                    "log_spec_idx": bool(i % 2),
                    "ref_freq": 100e6 + 1e6 * i,
                    "q_pol": 2.0 * i,
                    "u_pol": -1.1 * i,
                    "v_pol": 0.1 * i,
                },
            )

        # Set a couple of header key, value pairs (as comments).
        header = {
            "QUERY_PARAM_1": "PARAM_1_VALUE",
            "QUERY_PARAM_2": 42,
        }
        model.set_header(header)

        # Set special metadata values.
        # These have specific places in the YAML file
        # (currently, just the execution block ID).
        execution_block_id = "eb-test-write-lsm"
        model.set_metadata(
            {
                "execution_block_id": execution_block_id,
            }
        )

        # Write the model to a CSV file.
        with tempfile.TemporaryDirectory() as temp_dir_name:
            csv_file_names = [
                os.path.join(temp_dir_name, "_temp_test_lsm1.csv"),
                os.path.join(temp_dir_name, "_temp_test_lsm2.csv"),
            ]
            yaml_dir_name = os.path.join(temp_dir_name, "_temp_test_yaml_metadata_dir")
            yaml_path = os.path.join(yaml_dir_name, "ska-data-product.yaml")

            # Save two copies of the LSM so we have two entries in the YAML.
            for csv_file_name in csv_file_names:
                model.save(path=csv_file_name, metadata_dir=yaml_dir_name)

            # Load the CSV file into a new model.
            model2 = LocalSkyModel.load(csv_file_names[0])
            assert model2.num_rows == num_rows

            # Check that the values were read correctly.
            for i in range(model2.num_rows):
                assert model2["component_id"][i] == f"GLEAM J{10 * i:06d}-{i:06d}"
                assert model2["ra"][i] == pytest.approx(11.0 * i)
                assert model2["dec"][i] == pytest.approx(-i)
                assert model2["i_pol"][i] == pytest.approx(10.0 * i)
                assert bool(model2["log_spec_idx"][i]) == bool(i % 2)
                assert numpy.allclose(model2["spec_idx"][i], [-0.7, i / 100.0, 0.123])
                assert model2["ref_freq"][i] == pytest.approx(100e6 + 1e6 * i)
                assert model2["q_pol"][i] == pytest.approx(2.0 * i)
                assert model2["u_pol"][i] == pytest.approx(-1.1 * i)
                assert model2["v_pol"][i] == pytest.approx(0.1 * i)

            # Check that the metadata YAML file was written correctly.
            with open(yaml_path, encoding="utf-8") as stream:
                metadata = yaml.safe_load(stream)

            # Check the entry for each file.
            for i, csv_file_name in enumerate(csv_file_names):
                lsm_dict = metadata["local_sky_model"][i]
                assert lsm_dict["columns"] == column_names
                assert lsm_dict["file_path"] == csv_file_name
                assert lsm_dict["header"]["QUERY_PARAM_1"] == header["QUERY_PARAM_1"]
                assert lsm_dict["header"]["QUERY_PARAM_2"] == header["QUERY_PARAM_2"]
                assert lsm_dict["header"]["NUMBER_OF_COMPONENTS"] == num_rows
                assert metadata["execution_block"] == execution_block_id
                assert metadata["files"][i]["path"] == csv_file_name

    def test_tokenize_line(self):
        """Test the _tokenize_line method with various inputs."""
        # Test simple comma-separated values
        tokens = LocalSkyModel._tokenize_line("a,b,c")
        assert tokens == ["a", "b", "c"]

        # Test with spaces
        tokens = LocalSkyModel._tokenize_line("  a  ,  b  ,  c  ")
        assert tokens == ["a", "b", "c"]

        # Test with quoted strings
        tokens = LocalSkyModel._tokenize_line('"quoted,value",normal,value')
        assert tokens == ['"quoted,value"', "normal", "value"]

        # Test with bracketed vectors
        tokens = LocalSkyModel._tokenize_line("id,[1.0,2.0,3.0],4.5")
        assert tokens == ["id", "[1.0,2.0,3.0]", "4.5"]

        # Test with trailing comma
        tokens = LocalSkyModel._tokenize_line("a,b,")
        assert tokens == ["a", "b", ""]

        # Test with None
        tokens = LocalSkyModel._tokenize_line(None)
        assert not tokens

    def test_normalize_value(self):
        """Test the _normalize_value method."""
        model = LocalSkyModel.empty(["test_col"], 1)

        # Test empty string becomes None
        assert model._normalize_value("") is None
        assert model._normalize_value("  ") is None

        # Test non-empty strings are preserved
        assert model._normalize_value("test") == "test"
        assert model._normalize_value("  test  ") == "test"

        # Test non-string values pass through
        assert model._normalize_value(123) == 123
        assert model._normalize_value(None) is None

    def test_set_typed_values(self):
        """Test the typed setter methods for each column type."""
        # Create model with properly typed columns
        column_names = ["component_id", "ra", "count", "log_spec_idx", "spec_idx"]
        model = LocalSkyModel.empty(
            column_names,
            3,
            vector_columns=["spec_idx"],
        )

        # Test float setter (ra is recognized as float)
        model._set_float_value("ra", 0, "3.14")
        assert model["ra"][0] == pytest.approx(3.14)
        model._set_float_value("ra", 1, "invalid")
        assert numpy.isnan(model["ra"][1])

        # Test int setter (would need a column that's recognized as int by schema)
        # Since no default int columns exist, we'll test the method directly on a float array
        # by temporarily using it, which will fail but we can verify the exception handling

        # Test bool setter (log_spec_idx is recognized as bool)
        model._set_bool_value("log_spec_idx", 0, "true")
        assert model["log_spec_idx"][0] == 1
        model._set_bool_value("log_spec_idx", 1, "false")
        assert model["log_spec_idx"][1] == 0
        model._set_bool_value("log_spec_idx", 2, None)
        assert model["log_spec_idx"][2] == model.BOOL_MISSING

        # Test string setter (component_id is recognized as str)
        model._set_str_value("component_id", 0, "test")
        assert model["component_id"][0] == "test"
        model._set_str_value("component_id", 1, None)
        assert model["component_id"][1] == ""

        # Test vector setter (spec_idx is recognized as vector_float)
        model._set_vector_value("spec_idx", 0, [1.0, 2.0, 3.0])
        assert numpy.allclose(model["spec_idx"][0], [1.0, 2.0, 3.0])

    def test_format_column_values(self):
        """Test the column value formatting methods."""
        column_names = ["component_id", "ra", "log_spec_idx", "spec_idx"]
        model = LocalSkyModel.empty(
            column_names,
            1,
            vector_columns=["spec_idx"],
        )

        # Set test values using proper column types
        model.set_value("ra", 0, 3.14159)
        model.set_value("log_spec_idx", 0, "true")
        model.set_value("component_id", 0, "test")
        model.set_value("spec_idx", 0, [1.0, 2.0, 3.0])

        # Test formatters
        assert model._format_float_value("ra", 0) == "3.14159"
        assert model._format_bool_value("log_spec_idx", 0) == "true"
        assert model._format_vector_column_value("spec_idx", 0) == "[1,2,3]"

        # Test missing values
        model2 = LocalSkyModel.empty(["ra", "log_spec_idx"], 1)
        assert model2._format_float_value("ra", 0) == ""
        assert model2._format_bool_value("log_spec_idx", 0) == ""

    def test_handle_quote(self):
        """Test the _handle_quote static method."""
        # Test entering quotes
        in_quotes, quote_char = LocalSkyModel._handle_quote('"', False, None)
        assert in_quotes is True
        assert quote_char == '"'

        # Test exiting quotes
        in_quotes, quote_char = LocalSkyModel._handle_quote('"', True, '"')
        assert in_quotes is False
        assert quote_char is None

        # Test inside quotes with different char
        in_quotes, quote_char = LocalSkyModel._handle_quote('"', True, "'")
        assert in_quotes is True
        assert quote_char == "'"

    def test_handle_bracket(self):
        """Test the _handle_bracket static method."""
        # Test opening bracket
        handled, depth = LocalSkyModel._handle_bracket("[", 0)
        assert handled is True
        assert depth == 1

        # Test closing bracket
        handled, depth = LocalSkyModel._handle_bracket("]", 1)
        assert handled is True
        assert depth == 0

        # Test non-bracket
        handled, depth = LocalSkyModel._handle_bracket("x", 0)
        assert handled is False
        assert depth == 0

    def test_is_separator(self):
        """Test the _is_separator static method."""
        # Test comma outside quotes and brackets
        assert LocalSkyModel._is_separator(",", False, 0) is True

        # Test comma inside quotes
        assert LocalSkyModel._is_separator(",", True, 0) is False

        # Test comma inside brackets
        assert LocalSkyModel._is_separator(",", False, 1) is False

        # Test non-comma
        assert LocalSkyModel._is_separator("x", False, 0) is False
