"""Common utilities for the cli"""

import argparse


class SkaSdpFormatter(
    argparse.ArgumentDefaultsHelpFormatter,
    argparse.RawDescriptionHelpFormatter,
):
    """SKA SDP Formatter such that we can use both the defaults view
    and have raw descriptions shown."""
