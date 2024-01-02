import os
import sys
import types
import logging
import importlib


def import_filepath(filepath, module_name=None):
    """Import python file as python module.

    Python 2 and Python 3 compatibility.

    Args:
        filepath(str): Path to python file.
        module_name(str): Name of loaded module. Only for Python 3. By default
            is filled with filename of filepath.
    """
    if module_name is None:
        module_name = os.path.splitext(os.path.basename(filepath))[0]

    # Make sure it is not 'unicode' in Python 2
    module_name = str(module_name)

    # Prepare module object where content of file will be parsed
    module = types.ModuleType(module_name)

    # Use loader so module has full specs
    module_loader = importlib.machinery.SourceFileLoader(
        module_name, filepath
    )
    module_loader.exec_module(module)
    return module


def modules_from_path(folder_path, log=None):
    """Get python scripts as modules from a path.

    Arguments:
        folder_path (str): Path to folder containing python scripts.
        log (Optional[logging.Logger]): Logger used for logs.

    Returns:
        tuple<list, list>: First list contains successfully imported modules
            and second list contains tuples of path and exception.
    """
    crashed = []
    modules = []
    output = (modules, crashed)
    # Just skip and return empty list if path is not set
    if not folder_path:
        return output

    if log is None:
        log = logging.getLogger("modules_from_path")
    # Do not allow relative imports
    if folder_path.startswith("."):
        log.warning((
            "BUG: Relative paths are not allowed for security reasons. {}"
        ).format(folder_path))
        return output

    folder_path = os.path.normpath(folder_path)

    if not os.path.isdir(folder_path):
        log.warning("Not a directory path: {}".format(folder_path))
        return output

    for filename in os.listdir(folder_path):
        # Ignore files which start with underscore
        if filename.startswith("_"):
            continue

        mod_name, mod_ext = os.path.splitext(filename)
        if not mod_ext == ".py":
            continue

        full_path = os.path.join(folder_path, filename)
        if not os.path.isfile(full_path):
            continue

        try:
            module = import_filepath(full_path, mod_name)
            modules.append((full_path, module))

        except Exception:
            crashed.append((full_path, sys.exc_info()))
            log.warning(
                "Failed to load path: \"{0}\"".format(full_path),
                exc_info=True
            )
            continue

    return output
