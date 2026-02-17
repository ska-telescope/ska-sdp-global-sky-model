#!/usr/bin/env python
"""Script to import a dataset directly from disk"""

import argparse
from pathlib import Path
import sys

from ska_sdp_global_sky_model.api.app.ingest import ingest_catalogue
from ska_sdp_global_sky_model.api.app.models import GlobalSkyModelMetadata
from ska_sdp_global_sky_model.configuration.config import get_db


def main():
    """Main import script"""
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--name", help="Name of dataset", default="Sample Dataset")
    parser.add_argument("--catalogue_name", help="Name of catalogue", default="test_catalogue")
    parser.add_argument("--version", help="Catalogue version", default="0.1.0")
    parser.add_argument("csv_files", help="CSV Files to include", nargs="+")

    args = parser.parse_args()

    # Configuration pointing to the in-memory content
    catalogue_config = {
        "name": args.name,
        "catalogue_name": args.catalogue_name,
        "ingest": {"file_location": []},
    }

    for file in args.csv_files:
        catalogue_config["ingest"]["file_location"].append(
            {"content": Path(file).read_text(encoding="utf8")}
        )

    # Get DB session and load the data
    db = next(get_db())
    
    # Create metadata entry for this catalogue
    metadata = GlobalSkyModelMetadata(
        catalogue_name=args.catalogue_name,
        version=args.version,
        name=args.name,
    )
    db.add(metadata)
    db.commit()
    
    if not ingest_catalogue(db, catalogue_config):
        sys.exit(1)


if __name__ == "__main__":
    main()
