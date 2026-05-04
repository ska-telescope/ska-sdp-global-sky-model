"""Delete a catalogue"""

import argparse
import logging
import sys
from importlib.metadata import version

import ska_ser_logging

from ska_sdp_global_sky_model.api.app.models import (
    GlobalSkyModelMetadata,
    SkyComponent,
    SkyComponentStaging,
)
from ska_sdp_global_sky_model.configuration.config import (
    get_db,
)

ska_ser_logging.configure_logging(level="INFO")

logger = logging.getLogger(__name__)

# pylint: disable=duplicate-code


def main():
    """Main delete script"""
    parser = argparse.ArgumentParser(
        description=__doc__,
        epilog=f"Version: {version('ska_sdp_global_sky_model')}",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--delete", help="Do the actual delete", action="store_true")
    parser.add_argument("catalogue_id", help="The numerical ID of a catalogue", type=int)
    args = parser.parse_args()

    db = next(get_db())

    catalogues = (
        db.query(GlobalSkyModelMetadata)
        .filter(GlobalSkyModelMetadata.id == args.catalogue_id)
        .all()
    )

    if len(catalogues) == 0:
        logger.error("No catalogues found for ID '%s'", args.catalogue_id)
        sys.exit(1)

    if len(catalogues) > 1:
        logger.warning("More than 1 catalogue found")

    for catalogue in catalogues:
        logger.info(
            "Catalogue: '%s' (uploaded @ '%s') (Staging:%s)",
            catalogue.catalogue_name,
            catalogue.uploaded_at,
            "yes" if catalogue.staging else "no",
        )
        # Count components
        component_count = (
            db.query(SkyComponent).filter(SkyComponent.gsm_id == catalogue.id).count()
        )
        logger.info("Found %d components", component_count)
        component_count = (
            db.query(SkyComponentStaging)
            .filter(SkyComponentStaging.gsm_id == catalogue.id)
            .count()
        )
        logger.info("Found %d components in staging table", component_count)

        if args.delete:
            logger.warning("Deleting catalogue...")
            db.query(SkyComponentStaging).filter(
                SkyComponentStaging.gsm_id == catalogue.id
            ).delete()
            db.query(SkyComponent).filter(SkyComponent.gsm_id == catalogue.id).delete()
            db.delete(catalogue)
            db.commit()


if __name__ == "__main__":
    main()
