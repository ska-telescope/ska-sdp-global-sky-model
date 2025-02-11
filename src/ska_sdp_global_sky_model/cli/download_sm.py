"""Ingest the given data source."""

import logging
import shutil
from pathlib import Path

from ska_telmodel.data import TMData

from ska_sdp_global_sky_model.cli.common_cli import create_last_update, get_args, setup_parser
from ska_sdp_global_sky_model.configuration.config import DATASET_ROOT, TMDATA_SOURCE
from ska_sdp_global_sky_model.utilities.helper_functions import download_and_extract_file

logger = logging.getLogger(__name__)


def main():
    """Main function."""

    parser = setup_parser(__doc__)
    parser.add_argument("files", help="Local file or TMData link", nargs="+")
    args = get_args(parser)

    logger.info("Using dir: %s", DATASET_ROOT)

    tmdata = TMData([TMDATA_SOURCE])

    for file in args.files:
        path = Path(file)
        if path.exists():
            shutil.copy(path, DATASET_ROOT / path.name)
            download_and_extract_file(None, DATASET_ROOT / path.name)
        else:
            download_link = tmdata.get(file).get_link_contents()["downloadUrl"]
            download_and_extract_file(download_link, DATASET_ROOT / file.split("/")[-1])

    create_last_update()


if __name__ == "__main__":
    main()
