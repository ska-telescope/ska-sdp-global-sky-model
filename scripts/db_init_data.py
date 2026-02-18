#!/usr/bin/env python
"""Script to import a dataset directly from disk"""

import argparse
import json
import sys
import uuid
from pathlib import Path

from ska_sdp_global_sky_model.api.app.ingest import ingest_catalogue
from ska_sdp_global_sky_model.api.app.models import CatalogMetadata
from ska_sdp_global_sky_model.configuration.config import get_db


def main():
    """Main import script"""
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    print("Starting direct import script...")
    parser.add_argument(
        "--ignore-import-failure", help="Don't exit with error on failure", action="store_true"
    )
    parser.add_argument(
        "--metadata-file",
        required=True,
        help="Path to catalog metadata JSON file (contains version, catalogue_name, etc.)",
    )
    parser.add_argument("csv_files", help="CSV files to include", nargs="+")

    args = parser.parse_args()

    # Load metadata from JSON file
    metadata_path = Path(args.metadata_file)
    if not metadata_path.exists():
        print(f"Error: Metadata file not found: {metadata_path}")
        sys.exit(1)

    with metadata_path.open("r", encoding="utf-8") as f:
        metadata_json = json.load(f)

    # Validate required fields
    required_fields = ["version", "catalogue_name", "ref_freq", "epoch"]
    missing_fields = [field for field in required_fields if field not in metadata_json]
    if missing_fields:
        print(f"Error: Missing required fields in metadata file: {missing_fields}")
        sys.exit(1)

    # Get DB session
    db = next(get_db())

    try:
        # Generate unique upload_id for this import
        upload_id = f"init-{uuid.uuid4()}"

        # Create catalog metadata entry
        catalog_metadata = CatalogMetadata(
            version=metadata_json["version"],
            catalogue_name=metadata_json["catalogue_name"],
            description=metadata_json.get(
                "description", f"Import of {metadata_json['catalogue_name']}"
            ),
            upload_id=upload_id,
            ref_freq=metadata_json["ref_freq"],
            epoch=metadata_json["epoch"],
            author=metadata_json.get("author"),
            reference=metadata_json.get("reference"),
            notes=metadata_json.get("notes"),
        )
        db.add(catalog_metadata)
        db.commit()

        print(
            f"Created catalog metadata: {metadata_json['catalogue_name']} "
            f"v{metadata_json['version']}"
        )

        # Build ingestion metadata structure
        ingest_metadata = {
            "version": metadata_json["version"],
            "catalogue_name": metadata_json["catalogue_name"],
            "description": metadata_json.get("description"),
            "ref_freq": metadata_json["ref_freq"],
            "epoch": metadata_json["epoch"],
            "upload_id": upload_id,
            "staging": False,  # Direct import goes to main table
            "ingest": {"file_location": []},
        }

        # Load CSV file contents
        for csv_file in args.csv_files:
            csv_path = Path(csv_file)
            if not csv_path.exists():
                print(f"Error: CSV file not found: {csv_path}")
                sys.exit(1)

            ingest_metadata["ingest"]["file_location"].append(
                {"content": csv_path.read_text(encoding="utf-8")}
            )

        print(f"Ingesting {len(args.csv_files)} CSV file(s)...")

        # Ingest the catalogue data
        if not ingest_catalogue(db, ingest_metadata):
            print("Error: Catalog ingestion failed")
            if not args.ignore_import_failure:
                sys.exit(1)

        print(
            f"Successfully imported {metadata_json['catalogue_name']} "
            f"v{metadata_json['version']}"
        )

    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"Error during import: {e}")
        db.rollback()
        if not args.ignore_import_failure:
            sys.exit(1)
    finally:
        db.close()


if __name__ == "__main__":
    main()
