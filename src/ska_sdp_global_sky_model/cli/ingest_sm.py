"""Ingest the given data source."""

import logging
from pathlib import Path

from ska_sdp_global_sky_model.cli.common_cli import create_last_update, get_args, setup_parser
from ska_sdp_global_sky_model.cli.ingest import get_full_catalog
from ska_sdp_global_sky_model.configuration.config import DATASET_ROOT, DATASTORE, MWA, RACS, RCAL

logger = logging.getLogger(__name__)


def main():
    """Main function."""

    parser = setup_parser(__doc__)
    parser.add_argument("catalogues", help="Either the name or path to a catalogue", nargs="+")
    args = get_args(parser)

    logger.info("Using dir: %s", DATASET_ROOT)

    for catalogue in args.catalogues:
        try:
            if catalogue == "gleam":
                get_full_catalog(DATASTORE, MWA)
            elif catalogue == "racs":
                get_full_catalog(DATASTORE, RACS)
            else:
                path = Path(catalogue)
                if path.exists() and path.is_file():
                    rcal_config = RCAL.copy()
                    rcal_config["ingest"]["file_location"][0]["key"] = path
                    get_full_catalog(DATASTORE, rcal_config)
                else:
                    logger.error("Path does not exist: %s", path)
        except FileNotFoundError as exc:
            logger.error("Catalogue '%s' import failed: %s", catalogue, exc)

    create_last_update()


if __name__ == "__main__":
    main()
