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


def run_both():
    both_idx = sys.argv.index("both")
    leecher_args = list(sys.argv)
    processor_args = list(sys.argv)

    leecher_args[both_idx] = "leecher"
    processor_args[both_idx] = "processor"

    leecher_args.insert(0, sys.executable)
    processor_args.insert(0, sys.executable)

    leecher = subprocess.Popen(leecher_args)
    processor = subprocess.Popen(processor_args)
    try:
        while True:
            l_poll = leecher.poll()
            p_poll = processor.poll()
            if l_poll is not None and p_poll is not None:
                break

            if p_poll is None:
                if l_poll is not None:
                    processor.kill()

            if l_poll is None:
                if p_poll is not None:
                    leecher.kill()

            time.sleep(0.1)
    finally:
        if leecher.poll() is None:
            leecher.kill()

        if processor.poll() is None:
            processor.kill()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--service",
        help="Run processor service",
        choices=["processor", "leecher", "both"],
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
    if service_name == "both":
        return run_both()

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
