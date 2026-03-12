#!/usr/bin/env python
"""Script to import a dataset directly from disk"""

import logging
import argparse
import json
import sys
import uuid
from pathlib import Path

from ska_sdp_global_sky_model.api.app.ingest import ingest_catalogue
from ska_sdp_global_sky_model.api.app.models import GlobalSkyModelMetadata
from ska_sdp_global_sky_model.configuration.config import get_db

logger = logging.getLogger(__name__)


def main():
    """Main import script"""
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    logger.info("Starting direct import script...")
    parser.add_argument(
        "--metadata-file",
        required=True,
        help="Path to catalogue metadata JSON file (contains version, catalogue_name, etc.)",
    )
    parser.add_argument("csv_files", help="CSV Files to include", nargs="+")
    parser.add_argument(
        "--ignore-import-failure", help="Don't exit with error on failure", action="store_true"
    )

    args = parser.parse_args()

    # Load metadata from JSON file
    metadata_path = Path(args.metadata_file)
    if not metadata_path.exists():
        logger.error("Error: Metadata file not found: %s", metadata_path)
        sys.exit(1)

    with metadata_path.open("r", encoding="utf-8") as f:
        metadata_json = json.load(f)

    # Validate required fields
    required_fields = ["version", "catalogue_name", "epoch"]
    missing_fields = [field for field in required_fields if field not in metadata_json]
    if missing_fields:
        logger.error("Error: Missing required fields in metadata file: %s", missing_fields)
        sys.exit(1)

    # Get DB session
    db = next(get_db())

    try:
        # Generate unique upload_id for this import
        upload_id = f"init-{uuid.uuid4()}"

        # Create catalogue metadata entry
        global_sky_model_metadata = GlobalSkyModelMetadata(
            version=metadata_json["version"],
            catalogue_name=metadata_json["catalogue_name"],
            description=metadata_json.get(
                "description", f"Import of {metadata_json['catalogue_name']}"
            ),
            upload_id=upload_id,
            epoch=metadata_json["epoch"],
            author=metadata_json.get("author"),
            reference=metadata_json.get("reference"),
            notes=metadata_json.get("notes"),
            staging=False,
        )
        db.add(global_sky_model_metadata)
        db.commit()

        logger.info(
            "Created catalogue metadata: %s %s with staging=%s and upload_id=%s",
            global_sky_model_metadata.catalogue_name,
            global_sky_model_metadata.version,
            global_sky_model_metadata.staging,
            global_sky_model_metadata.upload_id,
        )

        # Build ingestion metadata structure
        catalogue_content = {
            "ingest": {"file_location": []},
        }

        # Load CSV file contents
        for csv_file in args.csv_files:
            csv_path = Path(csv_file)
            if not csv_path.exists():
                logger.error("Error: CSV file not found: %s", csv_path)
                sys.exit(1)

            catalogue_content["ingest"]["file_location"].append(
                {"content": csv_path.read_text(encoding="utf-8")}
            )

        logger.info("Ingesting %d CSV file(s)...", len(args.csv_files))

        # Ingest the catalogue data
        if not ingest_catalogue(db, global_sky_model_metadata, catalogue_content):
            logger.error("Error: Catalogue ingestion failed")
            if not args.ignore_import_failure:
                sys.exit(1)

        logger.info(
            "Successfully imported %s v%s",
            metadata_json["catalogue_name"],
            metadata_json["version"],
        )

    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.exception(e)
        logger.error("Error during import: %s", str(e))
        db.rollback()
        if not args.ignore_import_failure:
            sys.exit(1)
    finally:
        db.close()


if __name__ == "__main__":
    main()
