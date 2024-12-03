import os
import sys
import shutil
import uuid
import contextlib
import threading
import time
import json
import tarfile
import zipfile

import platformdirs
import ayon_api

IMPLEMENTED_ARCHIVE_FORMATS = {
    ".zip", ".tar", ".tgz", ".tar.gz", ".tar.xz", ".tar.bz2"
}
PROCESS_ID = uuid.uuid4().hex
# Wait 1 hour before cleaning up download dir
# - Running process should update lock file every 1 second
_LOCK_CLEANUP_TIME = 60 * 60


def get_download_root():
    root = os.getenv("AYON_FTRACK_DOWNLOAD_ROOT")
    if not root:
        root = os.path.join(
            platformdirs.user_data_dir("ayon-ftrack", "Ynput"),
            "downloads"
        )
    return root


def get_archive_ext_and_type(archive_file):
    """Get archive extension and type.

    Args:
        archive_file (str): Path to archive file.

    Returns:
        Tuple[str, str]: Archive extension and type.
    """

    tmp_name = archive_file.lower()
    if tmp_name.endswith(".zip"):
        return ".zip", "zip"

    for ext in (
        ".tar",
        ".tgz",
        ".tar.gz",
        ".tar.xz",
        ".tar.bz2",
    ):
        if tmp_name.endswith(ext):
            return ext, "tar"

    return None, None


def extract_archive_file(archive_file, dst_folder=None):
    """Extract archived file to a directory.

    Args:
        archive_file (str): Path to a archive file.
        dst_folder (Optional[str]): Directory where content will be extracted.
            By default, same folder where archive file is.

    """
    if not dst_folder:
        dst_folder = os.path.dirname(archive_file)

    os.makedirs(dst_folder, exist_ok=True)

    archive_ext, archive_type = get_archive_ext_and_type(archive_file)

    print(f"Extracting {archive_file} -> {dst_folder}")
    if archive_type is None:
        _, ext = os.path.splitext(archive_file)
        raise ValueError((
            f"Invalid file extension \"{ext}\"."
            f" Expected {', '.join(IMPLEMENTED_ARCHIVE_FORMATS)}"
        ))

    if archive_type == "zip":
        with zipfile.ZipFile(archive_file) as zip_file:
            zip_file.extractall(dst_folder)

    elif archive_type == "tar":
        if archive_ext == ".tar":
            tar_type = "r:"
        elif archive_ext.endswith(".xz"):
            tar_type = "r:xz"
        elif archive_ext.endswith(".gz"):
            tar_type = "r:gz"
        elif archive_ext.endswith(".bz2"):
            tar_type = "r:bz2"
        else:
            tar_type = "r:*"

        with tarfile.open(archive_file, tar_type) as tar_file:
            tar_file.extractall(dst_folder)


class _LockThread(threading.Thread):
    def __init__(self, lock_file):
        super().__init__()
        self.lock_file = lock_file
        self._event = threading.Event()
        self.interval = 1

    def stop(self):
        if not self._event.is_set():
            self._event.set()

    def run(self):
        with open(self.lock_file, "w") as stream:
            while not self._event.wait(self.interval):
                stream.seek(0)
                stream.write(str(time.time()))
                stream.flush()


@contextlib.contextmanager
def _lock_file_update(lock_file):
    thread = _LockThread(lock_file)
    thread.start()
    try:
        yield
    finally:
        thread.stop()
        thread.join()


def _download_event_handlers(dirpath, custom_handlers, event_handler_dirs):
    for custom_handler in custom_handlers:
        addon_name = custom_handler["addon_name"]
        addon_version = custom_handler["addon_version"]
        endpoint = custom_handler["endpoint"]
        filename = endpoint.rsplit("/")[-1]
        path = os.path.join(dirpath, filename)
        url = "/".join([ayon_api.get_base_url(), endpoint])
        try:
            ayon_api.download_file(url, path)

        except BaseException as exc:
            print(
                "Failed to download event handlers"
                f" for {addon_name} {addon_version}"
                f"from '{endpoint}'. Reason: {exc}"
            )
            continue

        try:
            # Create temp dir for event handlers
            subdir = f"{addon_name}_{addon_version}"
            extract_dir = os.path.join(dirpath, subdir)
            # Extract downloaded archive
            extract_archive_file(path, extract_dir)
            manifest_file = os.path.join(extract_dir, "manifest.json")
            if not os.path.exists(manifest_file):
                print(
                    f"Manifest file not found in"
                    f" downloaded archive from {endpoint}"
                )
                continue

            with open(manifest_file, "r") as stream:
                manifest = json.load(stream)

            manifest_version = manifest["version"]
            maj_v, min_v, patch_v = (
                int(part) for part in manifest_version.split(".")
            )
            if (maj_v, min_v, patch_v) > (1, 0, 0):
                print(
                    f"Manifest file has unknown version {manifest_version}."
                    " Trying to process it anyway."
                )
                continue

            for even_handler_subpath in manifest.get("handler_subfolders", []):
                # Add path to event handler dirs
                event_handler_dirs.append(os.path.join(
                    extract_dir, even_handler_subpath
                ))

            for python_subpath in manifest.get("python_path_subfolders", []):
                python_dir = os.path.join(extract_dir, python_subpath)
                sys.path.insert(0, python_dir)

        except BaseException as exc:
            print(f"Failed to extract downloaded archive: {exc}")

        finally:
            # Remove archive
            os.remove(path)


@contextlib.contextmanager
def downloaded_event_handlers(custom_handlers):
    event_handler_dirs = []
    if not custom_handlers:
        yield event_handler_dirs
        return

    root = get_download_root()
    dirpath = os.path.join(root, PROCESS_ID)
    os.makedirs(dirpath, exist_ok=True)

    lock_file = os.path.join(dirpath, "lock")
    try:
        with _lock_file_update(lock_file):
            print("Downloading event handlers...")
            _download_event_handlers(
                dirpath, custom_handlers, event_handler_dirs
            )
            yield event_handler_dirs
    finally:
        shutil.rmtree(dirpath)
        print("Cleaned up downloaded event handlers")


def cleanup_download_root():
    root = get_download_root()
    if not os.path.exists(root):
        return

    current_time = time.time()
    paths_to_remove = []
    for subdir in os.listdir(root):
        path = os.path.join(root, subdir)
        if not os.path.isdir(path):
            continue
        lock_file = os.path.join(path, "lock")
        if not os.path.exists(lock_file):
            paths_to_remove.append(path)
            continue

        try:
            with open(lock_file, "r") as stream:
                content = stream.read()

        except BaseException:
            print(
                "Failed to read lock file to check"
                f" if can remove downloaded content '{lock_file}'"
            )
            continue

        last_update = 0
        if content:
            last_update = float(content)
        if (current_time - last_update) > _LOCK_CLEANUP_TIME:
            paths_to_remove.append(path)

    for path in paths_to_remove:
        print(f"Cleaning up download directory: {path}")
        shutil.rmtree(path)
