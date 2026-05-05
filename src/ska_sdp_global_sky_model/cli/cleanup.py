"""Run cleanups of the catalogues"""

import argparse
from importlib.metadata import version

import ska_ser_logging

from ska_sdp_global_sky_model.api.app.upload_manager import UploadManager
from ska_sdp_global_sky_model.configuration.config import CATALOGUE_CLEANUP_AGE, get_db

ska_ser_logging.configure_logging(level="INFO")


def main(arg_list: list[str] | None = None):
    """Main cleanup script"""
    parser = argparse.ArgumentParser(
        description=__doc__,
        epilog=f"Version: {version('ska_sdp_global_sky_model')}",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--max-age",
        help="The maximum age of staged catalogues in hours",
        default=CATALOGUE_CLEANUP_AGE,
        type=int,
    )
    parser.add_argument("--delete", help="Commit the deletion", action="store_true")
    args = parser.parse_args(arg_list)

    db = next(get_db())
    upload_manager = UploadManager()
    upload_manager.run_db_cleanup(db, args.delete, args.max_age)


if __name__ == "__main__":  # pragma: no cover
    main()
