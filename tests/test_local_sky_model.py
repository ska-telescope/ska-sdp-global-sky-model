"""
This module contains tests for local_sky_model.py
"""

import os
import tempfile

import yaml

from ska_sdp_datamodels.global_sky_model import LocalSkyModel
from ska_sdp_global_sky_model.utilities.local_sky_model import save_lsm_with_metadata


def test_save_lsm_with_metadata():
    """
    Test that we can save a sky model and metadata YAML file correctly.
    """

    # Create an empty local sky model.
    column_names = ["ra_deg", "dec_deg", "i_pol_jy", "ref_freq_hz", "spec_idx"]
    num_rows = 20
    model = LocalSkyModel(column_names=column_names, num_rows=num_rows)

    # Set a couple of header key, value pairs (as comments).
    header = {
        "QUERY_PARAM_1": "PARAM_1_VALUE",
        "QUERY_PARAM_2": 42,
    }
    model.set_header(header)

    # Write the CSV and the YAML metadata files.
    with tempfile.TemporaryDirectory() as temp_dir_name:
        csv_file_names = [
            os.path.join(temp_dir_name, "_temp_test_lsm1.csv"),
            os.path.join(temp_dir_name, "_temp_test_lsm2.csv"),
        ]
        yaml_dir_name = os.path.join(temp_dir_name, "_temp_test_yaml_metadata_dir")
        yaml_path = os.path.join(yaml_dir_name, "ska-data-product.yaml")

        # Save two copies of the LSM so we have two entries in the YAML.
        execution_block_id = "eb-test-write-lsm"
        for csv_file_name in csv_file_names:
            save_lsm_with_metadata(
                model, {"execution_block_id": execution_block_id}, csv_file_name, yaml_dir_name
            )

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
