"""Ingest the given data source."""

import argparse
import logging
from pathlib import Path

import ska_ser_logging

from ska_sdp_global_sky_model.cli.ingest import get_full_catalog
from ska_sdp_global_sky_model.configuration.config import DATASET_ROOT, DATASTORE, MWA, RACS, RCAL

logger = logging.getLogger(__name__)


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--verbose", help="Output verbose", action="store_true", default=False)
    parser.add_argument("--debug", help="Output debug logs", action="store_true", default=False)
    parser.add_argument("catalogues", help="Either the name or path to a catalogue", nargs="+")
    args = parser.parse_args()
    level = logging.WARNING
    if args.verbose:
        level = logging.INFO
    if args.debug:
        level = logging.DEBUG

    ska_ser_logging.configure_logging(level)

    logger.info("Using dir: %s", DATASET_ROOT)

    for catalog in args.catalogues:
        if catalog == "gleam":
            get_full_catalog(DATASTORE, MWA)
        elif catalog == "racs":
            get_full_catalog(DATASTORE, RACS)
        else:
            path = Path(catalog)
            if path.exists() and path.is_file():
                rcal_config = RCAL.copy()
                rcal_config["ingest"]["file_location"][0]["key"] = path
                get_full_catalog(rcal_config, MWA)
            else:
                logger.error("Path does not exist: %s", path)


if __name__ == "__main__":
    main()
