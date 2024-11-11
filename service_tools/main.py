import os
import sys
import argparse
import subprocess
import time
import collections

from ayon_api.constants import (
    DEFAULT_VARIANT_ENV_KEY,
)

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
ADDON_DIR = os.path.dirname(CURRENT_DIR)


def _fix_ftrack_common_import():
    # map 'common' modules to 'ftrack_common'
    import common

    common_dir = os.path.join(
        ADDON_DIR, "client", "ayon_ftrack", "common"
    )
    new_prefix = "ftrack_common"

    sys.modules[new_prefix] = common

    import_queue = collections.deque()
    import_queue.extend(os.listdir(common_dir))
    while import_queue:
        subpath = import_queue.popleft()
        if subpath.endswith(".py"):
            full_module_name = subpath[:-3].replace("/", ".")
            module_name = full_module_name.split("/")[-1]
            if module_name in ("__init__", "__main__"):
                continue
            new_name = f"{new_prefix}.{full_module_name}"
            sys.modules[new_name] = __import__(f"common.{full_module_name}")
            continue

        module_path = os.path.join(common_dir, subpath)
        if os.path.isdir(module_path):
            import_queue.extend(
                "/".join((subpath, filename))
                for filename in os.listdir(module_path)
            )


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

    # Set download root for service tools inside service tools
    download_root = os.getenv("AYON_FTRACK_DOWNLOAD_ROOT")
    if not download_root:
        os.environ["AYON_FTRACK_DOWNLOAD_ROOT"] = os.path.join(
            CURRENT_DIR, "downloads"
        )

    service_name = opts.service
    if service_name == "both":
        return run_both()

    for path in (
        os.path.join(ADDON_DIR, "client", "ayon_ftrack"),
        os.path.join(ADDON_DIR, "services", service_name),
        os.path.join(ADDON_DIR),
    ):
        sys.path.insert(0, path)

    _fix_ftrack_common_import()

    if service_name == "processor":
        from processor import main as service_main
    else:
        from leecher import main as service_main
    service_main()


if __name__ == "__main__":
    main()
