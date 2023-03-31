"""Prepares server package from addon repo to upload to server.

Requires Python3.9. (Or at least 3.8+).

This script should be called from cloned addon repo.

It will produce 'package' subdirectory which could be pasted into server
addon directory directly (eg. into `openpype4-backend/addons`).

Format of package folder:
ADDON_REPO/package/{addon name}/{addon version}

You can specify `--output_dir` in arguments to change output directory where
package will be created. Existing package directory will be always purged if
already present! This could be used to create package directly in server folder
if available.

Package contains server side files directly,
client side code zipped in `private` subfolder.
"""

import os
import sys
import re
import shutil
import argparse
import logging
import collections
import zipfile
from typing import Optional, Any

COMMON_DIR_NAME: str = "ftrack_common"

# Patterns of directories to be skipped for server part of addon
IGNORE_DIR_PATTERNS: list[re.Pattern] = [
    re.compile(pattern)
    for pattern in {
        # Skip directories starting with '.'
        r"^\.",
        # Skip any pycache folders
        "^__pycache__$"
    }
]

# Patterns of files to be skipped for server part of addon
IGNORE_FILE_PATTERNS: list[re.Pattern] = [
    re.compile(pattern)
    for pattern in {
        # Skip files starting with '.'
        # NOTE this could be an issue in some cases
        r"^\.",
        # Skip '.pyc' files
        r"\.pyc$"
    }
]


def safe_copy_file(src_path: str, dst_path: str):
    """Copy file and make sure destination directory exists.

    Ignore if destination already contains directories from source.

    Args:
        src_path (str): File path that will be copied.
        dst_path (str): Path to destination file.
    """

    if src_path == dst_path:
        return

    dst_dir: str = os.path.dirname(dst_path)
    try:
        os.makedirs(dst_dir)
    except Exception:
        pass

    shutil.copy2(src_path, dst_path)


def _value_match_regexes(value: str, regexes: list[re.Pattern]) -> bool:
    for regex in regexes:
        if regex.search(value):
            return True
    return False


def find_files_in_subdir(
    src_path: str,
    ignore_file_patterns: Optional[list[re.Pattern]] = None,
    ignore_dir_patterns: Optional[list[re.Pattern]] = None
) -> list[tuple[str, str]]:
    if ignore_file_patterns is None:
        ignore_file_patterns: list[re.Pattern] = IGNORE_FILE_PATTERNS

    if ignore_dir_patterns is None:
        ignore_dir_patterns: list[re.Pattern] = IGNORE_DIR_PATTERNS
    output: list[tuple[str, str]] = []

    hierarchy_queue: collections.deque[tuple[str, list[str]]] = (
        collections.deque()
    )
    hierarchy_queue.append((src_path, []))
    while hierarchy_queue:
        item = hierarchy_queue.popleft()
        dirpath, parents = item
        for name in os.listdir(dirpath):
            path = os.path.join(dirpath, name)
            if os.path.isfile(path):
                if not _value_match_regexes(name, ignore_file_patterns):
                    items = list(parents)
                    items.append(name)
                    output.append((path, os.path.sep.join(items)))
                continue

            if not _value_match_regexes(name, ignore_dir_patterns):
                items = list(parents)
                items.append(name)
                hierarchy_queue.append((path, items))

    return output


def copy_server_content(
    addon_output_dir: str,
    current_dir: str,
    log: logging.Logger
):
    """Copies server side folders to 'addon_package_dir'

    Args:
        addon_output_dir (str): package dir in addon repo dir
        current_dir (str): addon repo dir
        log (logging.Logger)
    """

    log.info("Copying server content")

    server_dir: str = os.path.join(current_dir, "server")
    services_dir: str = os.path.join(current_dir, "services")
    common_dir: str = os.path.join(current_dir, COMMON_DIR_NAME)

    # Copy ftrack common to 'processor' service
    dst_processor_dir: str = os.path.join(
        addon_output_dir,
        "services",
        "processor",
        COMMON_DIR_NAME
    )

    filepaths_to_copy: list[tuple[str, str]] = [
        (
            os.path.join(current_dir, "version.py"),
            os.path.join(addon_output_dir, "version.py")
        ),
        # Copy constants needed for attributes creation
        (
            os.path.join(common_dir, "constants.py"),
            os.path.join(addon_output_dir, "constants.py")
        ),
    ]

    for path, sub_path in find_files_in_subdir(server_dir):
        filepaths_to_copy.append(
            (path, os.path.join(addon_output_dir, sub_path))
        )

    for path, sub_path in find_files_in_subdir(services_dir):
        filepaths_to_copy.append(
            (path, os.path.join(addon_output_dir, "services", sub_path))
        )

    for path, sub_path in find_files_in_subdir(common_dir):
        filepaths_to_copy.append(
            (path, os.path.join(dst_processor_dir, sub_path))
        )

    # Copy files
    for src_path, dst_path in filepaths_to_copy:
        safe_copy_file(src_path, dst_path)


def zip_client_side(
    addon_package_dir: str,
    current_dir: str,
    log: logging.Logger,
    zip_basename: Optional[str] = None
):
    """Copy and zip `client` content into `addon_package_dir'.

    Args:
        addon_package_dir (str): Output package directory path.
        current_dir (str): Directoy path of addon source.
        zip_basename (str): Output zip file name in format. 'client' by
            default.
        log (logging.Logger): Logger object.
    """

    client_dir: str = os.path.join(current_dir, "client")
    if not os.path.isdir(client_dir):
        log.info("Client directory was not found. Skipping")
        return

    if not zip_basename:
        zip_basename = "client"
    log.info("Preparing client code zip")
    private_dir: str = os.path.join(addon_package_dir, "private")
    if not os.path.exists(private_dir):
        os.makedirs(private_dir)

    common_dir: str = os.path.join(current_dir, COMMON_DIR_NAME)
    version_filepath: str = os.path.join(current_dir, "version.py")
    addon_subdir_name: str = "ayon_ftrack"

    zip_filename: str = zip_basename + ".zip"
    zip_filepath: str = os.path.join(os.path.join(private_dir, zip_filename))
    with zipfile.ZipFile(zip_filepath, "w", zipfile.ZIP_DEFLATED) as zipf:
        for path, sub_path in find_files_in_subdir(client_dir):
            zipf.write(path, sub_path)

        for path, sub_path in find_files_in_subdir(common_dir):
            zipf.write(path, "/".join((addon_subdir_name, "common", sub_path)))

        zipf.write(
            version_filepath, os.path.join(addon_subdir_name, "version.py")
        )


def main(output_dir: Optional[str] = None):
    addon_name: str = "ftrack"
    log: logging.Logger = logging.getLogger("create_package")
    log.info("Start creating package")

    current_dir: str = os.path.dirname(os.path.abspath(__file__))
    if not output_dir:
        output_dir = os.path.join(current_dir, "package")

    version_filepath: str = os.path.join(current_dir, "version.py")
    version_content: dict[str, Any] = {}
    with open(version_filepath, "r") as stream:
        exec(stream.read(), version_content)
    addon_version: str = version_content["__version__"]

    new_created_version_dir: str = os.path.join(
        output_dir, addon_name, addon_version
    )
    if os.path.isdir(new_created_version_dir):
        log.info(f"Purging {new_created_version_dir}")
        shutil.rmtree(output_dir)

    log.info(f"Preparing package for {addon_name}-{addon_version}")

    addon_output_dir: str = os.path.join(output_dir, addon_name, addon_version)
    if not os.path.exists(addon_output_dir):
        os.makedirs(addon_output_dir)

    copy_server_content(addon_output_dir, current_dir, log)

    zip_client_side(addon_output_dir, current_dir, log)


if __name__ == "__main__":
    parser: argparse.ArgumentParser = argparse.ArgumentParser()
    parser.add_argument(
        "--output_dir",
        help=(
            "Directory path where package will be created"
            " (Will be purged if already exists!)"
        )
    )

    args: argparse.Namespace = parser.parse_args(sys.argv[1:])
    main(args.output_dir)
