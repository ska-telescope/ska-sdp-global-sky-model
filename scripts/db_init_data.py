#!/usr/bin/env python
"""Script to import a dataset directly from disk"""

import argparse
from pathlib import Path
import sys

from ska_sdp_global_sky_model.api.app.ingest import get_full_catalog
from ska_sdp_global_sky_model.configuration.config import get_db


def main():
    """Main import script"""
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--name", help="Name of dataset", default="Sample Dataset")
    parser.add_argument("--catalog-name", help="Name of catalogue", default="test_catalog")
    parser.add_argument("--source", help="Source name", default="test_catalog")
    parser.add_argument("csv-files", help="CSV Files to include", nargs="+")

    args = parser.parse_args()

    # Configuration pointing to the in-memory content
    catalog_config = {
        "name": args.name,
        "catalog_name": args.catalog_name,
        "source": args.source,
        "ingest": {"file_location": []},
    }

    for file in args.csv_files:
        catalog_config["ingest"]["file_location"].append({"content": Path(file).read_bytes()})

    # Get DB session and load the data
    db = next(get_db())
    if not get_full_catalog(db, catalog_config):
        sys.exit(1)


if __name__ == "__main__":
    main()
