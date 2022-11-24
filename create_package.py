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

# Files or directories that won't be copied to server part of addon
IGNORED_FILENAMES = {
    "package",
    "__pycache__",
    "client",
    "create_package.py",
    "LICENSE",
}

# Patterns of directories to be skipped for server part of addon
IGNORE_DIR_PATTERNS = [
    re.compile(pattern)
    for pattern in {r"^\."}
]

# Patterns of files to be skipped for server part of addon
IGNORE_FILE_PATTERNS = [
    re.compile(pattern)
    for pattern in {r"^\.", r"\.pyc$"}
]


def _value_match_regexes(value, regexes):
    for regex in regexes:
        if regex.search(value):
            return True
    return False


def copy_server_content(addon_output_dir, current_dir, log):
    """Copies server side folders to 'addon_package_dir'

    Args:
        addon_output_dir (str): package dir in addon repo dir
        current_dir (str): addon repo dir
        log (logging.Logger)
    """

    log.info("Copying server content")

    for filename in os.listdir(current_dir):
        if filename in IGNORED_FILENAMES:
            continue

        src_path = os.path.join(current_dir, filename)
        dst_path = os.path.join(addon_output_dir, filename)
        if (
            os.path.isfile(src_path)
            and not _value_match_regexes(filename, IGNORE_FILE_PATTERNS)
        ):
            shutil.copy(src_path, dst_path)

        elif (
            os.path.isdir(src_path)
            and not _value_match_regexes(filename, IGNORE_DIR_PATTERNS)
        ):
            shutil.copytree(src_path, dst_path)


def zip_client_side(addon_package_dir, current_dir, zip_file_name, log):
    """Copy and zip `client` content into `addon_package_dir'.

    Args:
        addon_package_dir (str): package dir in addon repo dir
        current_dir (str): addon repo dir
        zip_file_name (str): file name in format {ADDON_NAME}_{ADDON_VERSION}
            (eg. 'sitesync_1.0.0')
        log (logging.Logger)
    """

    client_dir = os.path.join(current_dir, "client")
    if not os.path.isdir(client_dir):
        log.info("Client directory was not found. Skipping")
        return

    log.info("Preparing client code zip")
    private_dir = os.path.join(addon_package_dir, "private")
    temp_dir_to_zip = os.path.join(private_dir, "temp")
    # shutil.copytree expects glob-style patterns, not regex
    shutil.copytree(
        client_dir,
        os.path.join(temp_dir_to_zip, zip_file_name),
        ignore=shutil.ignore_patterns("*.pyc", "*__pycache__*")
    )

    toml_path = os.path.join(client_dir, "pyproject.toml")
    if os.path.exists(toml_path):
        shutil.copy(toml_path, private_dir)

    zip_file_path = os.path.join(os.path.join(private_dir, zip_file_name))
    shutil.make_archive(zip_file_path, "zip", temp_dir_to_zip)
    shutil.rmtree(temp_dir_to_zip)


def main(output_dir=None):
    addon_name = "ftrack"
    log = logging.getLogger("create_package")
    log.info("Start creating package")

    current_dir = os.path.dirname(os.path.abspath(__file__))
    if not output_dir:
        output_dir = os.path.join(current_dir, "package")

    version_filepath = os.path.join(current_dir, "version.py")
    version_content = {}
    with open(version_filepath, "r") as stream:
        exec(stream.read(), version_content)
    addon_version = version_content["__version__"]

    new_created_version_dir = os.path.join(
        output_dir, addon_name, addon_version
    )
    if os.path.isdir(new_created_version_dir):
        log.info(f"Purging {new_created_version_dir}")
        shutil.rmtree(output_dir)

    log.info(f"Preparing package for {addon_name}-{addon_version}")

    zip_file_name = f"{addon_name}_{addon_version}"
    addon_output_dir = os.path.join(output_dir, addon_name, addon_version)
    if not os.path.exists(addon_output_dir):
        os.makedirs(addon_output_dir)

    copy_server_content(addon_output_dir, current_dir, log)

    zip_client_side(addon_output_dir, current_dir, zip_file_name, log)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output_dir",
        help=(
            "Directory path where package will be created"
            " (Will be purged if already exists!)"
        )
    )

    args = parser.parse_args(sys.argv[1:])
    main(args.output_dir)
