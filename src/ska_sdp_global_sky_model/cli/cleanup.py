"""Run cleanups of the catalogues"""

import argparse
from importlib.metadata import version

import ska_ser_logging

from ska_sdp_global_sky_model.api.app.upload_manager import UploadManager
from ska_sdp_global_sky_model.configuration.config import (
    get_db,
)

ska_ser_logging.configure_logging(level="INFO")


class SkaSdpFormatter(
    argparse.ArgumentDefaultsHelpFormatter,
    argparse.RawDescriptionHelpFormatter,
):
    """SKA SDP Formatter such that we can use both the defaults view
    and have raw descriptions shown."""


def main():
    """Main cleanup script"""
    parser = argparse.ArgumentParser(
        description=__doc__,
        epilog=f"Version: {version('ska_sdp_global_sky_model')}",
        formatter_class=SkaSdpFormatter,
    )
    parser.add_argument(
        "--max-age", help="Ovverride the default maximum age of an upload", default=None
    )
    parser.add_argument("--dry-run", help="Perform dry run only", action="store_true")
    args = parser.parse_args()

    db = next(get_db())
    upload_manager = UploadManager()
    upload_manager.run_db_cleanup(db, args.dry_run, args.max_age)


if __name__ == "__main__":
    main()
