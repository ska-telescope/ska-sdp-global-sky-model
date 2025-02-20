"""Common function related to the CLI."""

import argparse
import logging
from time import time

import ska_ser_logging

from ska_sdp_global_sky_model.configuration.config import DATASET_ROOT


def create_last_update():
    """Create the last updated file."""

    if not DATASET_ROOT.exists():
        DATASET_ROOT.mkdir(parents=True, exist_ok=True)

    with (DATASET_ROOT / ".last_updated").open("w", encoding="utf8") as file:
        file.write(f"{time()}")


def setup_parser(doc: str) -> argparse.ArgumentParser:
    """Create the parser with default options."""

    parser = argparse.ArgumentParser(
        description=doc,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--verbose", help="Output verbose", action="store_true", default=False)
    parser.add_argument("--debug", help="Output debug logs", action="store_true", default=False)
    return parser


def get_args(parser: argparse.ArgumentParser) -> argparse.Namespace:
    """Parse the default parser, set up the logging, and return the resulting args."""

    args = parser.parse_args()
    level = logging.WARNING
    if args.verbose:
        level = logging.INFO
    if args.debug:
        level = logging.DEBUG

    ska_ser_logging.configure_logging(level)

    return args
