"""
Provides a function to save a local sky model with a YAML metadata file.
"""

import logging
import os
from typing import Any

from ska_sdp_datamodels.global_sky_model import LocalSkyModel
from ska_sdp_dataproduct_metadata import MetaData

LOGGER = logging.getLogger(__name__)


def save_lsm_with_metadata(
    lsm: LocalSkyModel, metadata_dict: dict[str, Any], lsm_path: str, metadata_dir: str
) -> None:
    """
    Save a sky model to a CSV text file, and update SKA data product metadata.

    Extra metadata is supplied in the metadata_dict parameter, which is
    a dictionary containing specific keys. The keys currently required are:

    - execution_block_id

    If the metadata file already exists, it will be updated with details
    of the new sky model file; otherwise, it will be created.

    An error will be raised during validation if both the YAML file and
    the LSM file already exist.
    To avoid the error, either delete the existing YAML file first,
    or ensure the LSM path given is unique.

    :param lsm: Local sky model to write.
    :type lsm: LocalSkyModel
    :param metadata_dict: Dictionary of metadata.
    :type metadata_dict: dict[str, Any]
    :param path: Path of CSV text file to write.
    :type path: str
    :param metadata_dir: Directory in which to write YAML metadata file.
    :type metadata_dir: str
    """
    # Save the CSV file.
    lsm.save(lsm_path)

    # Write or update the YAML metadata file.
    yaml_path = os.path.join(metadata_dir, "ska-data-product.yaml")
    if os.path.exists(yaml_path):
        # Open the existing file for update.
        metadata = MetaData(path=yaml_path)
        metadata.output_path = yaml_path
    else:
        # Create a new metadata file.
        metadata = MetaData()
        metadata.output_path = yaml_path

        # Write any special values that have been set
        # (e.g. the execution block ID).
        if "execution_block_id" in metadata_dict:
            metadata.set_execution_block_id(metadata_dict["execution_block_id"])

    # Get a handle to the top-level metadata dictionary.
    data = metadata.get_data()

    # Create the header dictionary.
    header = {
        "NUMBER_OF_COMPONENTS": lsm.num_components,
    }
    header.update(lsm.header)

    # Update the metadata contents.
    root = "local_sky_model"
    if root not in data:
        data[root] = []  # Ensure we have a list under the root item.

    # Create entry for new file in the list.
    data[root].append(
        {
            "header": header,
            "file_path": lsm_path,
            "columns": lsm.column_names,
        }
    )

    # Save the LSM file name in the metadata.
    # An error will be raised during validation if the LSM file already
    # exists. Ensure the LSM path given is unique.
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
