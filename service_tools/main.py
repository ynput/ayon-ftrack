import os
import sys
import logging
import argparse
import subprocess
import time

from ayon_api.constants import (
    DEFAULT_VARIANT_ENV_KEY,
)

ADDON_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--service",
        help="Run processor service",
        choices=["processor", "leecher"],
    )
    parser.add_argument(
        "--variant",
        default="production",
        help="Settings variant",
    )
    opts = parser.parse_args()
    if opts.variant:
        os.environ[DEFAULT_VARIANT_ENV_KEY] = opts.variant

    service_name = opts.service

    for path in (
        os.path.join(ADDON_DIR),
        os.path.join(ADDON_DIR, "services", service_name),
    ):
        sys.path.insert(0, path)

    if service_name == "processor":
        from processor import main as service_main
    else:
        from leecher import main as service_main
    service_main()


if __name__ == "__main__":
    logging.basicConfig()
    main()
