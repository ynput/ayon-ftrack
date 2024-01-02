import re
import numbers
import socket

import six
from ayon_api import (
    get_base_url,
    get_service_addon_name,
    get_service_addon_version,
)

from .exceptions import InvalidFpsValue


def join_filter_values(values):
    """Prepare values to be used for filtering in ftrack query.

    Args:
        Iterable[str]: Values to join for filter query.

    Returns:
        str: Prepared values for ftrack query.
    """

    return ",".join({
        '"{}"'.format(value)
        for value in values
    })


def create_chunks(iterable, chunk_size=None):
    """Separate iterable into multiple chunks by size.

    Args:
        iterable (Iterable[Any]): Object that will be separated into chunks.
        chunk_size (int): Size of one chunk. Default value is 200.

    Returns:
        List[List[Any]]: Chunked items.
    """

    chunks = []
    tupled_iterable = tuple(iterable)
    if not tupled_iterable:
        return chunks
    iterable_size = len(tupled_iterable)
    if chunk_size is None:
        chunk_size = 200

    if chunk_size < 1:
        chunk_size = 1

    for idx in range(0, iterable_size, chunk_size):
        chunks.append(tupled_iterable[idx:idx + chunk_size])
    return chunks


def is_string_number(value):
    """Can string value be converted to number (float)."""

    if not isinstance(value, six.string_types):
        raise TypeError("Expected {} got {}".format(
            ", ".join(str(t) for t in six.string_types), str(type(value))
        ))
    if value == ".":
        return False

    if value.startswith("."):
        value = "0" + value
    elif value.endswith("."):
        value = value + "0"

    if re.match(r"^\d+(\.\d+)?$", value) is None:
        return False
    return True


def convert_to_fps(source_value):
    """Convert value into fps value.

    Non string values are kept untouched. String is tried to convert.
    Valid values:
    "1000"
    "1000.05"
    "1000,05"
    ",05"
    ".05"
    "1000,"
    "1000."
    "1000/1000"
    "1000.05/1000"
    "1000/1000.05"
    "1000.05/1000.05"
    "1000,05/1000"
    "1000/1000,05"
    "1000,05/1000,05"

    Invalid values:
    "/"
    "/1000"
    "1000/"
    ","
    "."
    ...any other string

    Returns:
        float: Converted value.

    Raises:
        InvalidFpsValue: When value can't be converted to float.
    """

    if not isinstance(source_value, six.string_types):
        if isinstance(source_value, numbers.Number):
            return float(source_value)
        return source_value

    value = source_value.strip().replace(",", ".")
    if not value:
        raise InvalidFpsValue("Got empty value")

    subs = value.split("/")
    if len(subs) == 1:
        str_value = subs[0]
        if not is_string_number(str_value):
            raise InvalidFpsValue(
                "Value \"{}\" can't be converted to number.".format(value)
            )
        return float(str_value)

    elif len(subs) == 2:
        divident, divisor = subs
        if not divident or not is_string_number(divident):
            raise InvalidFpsValue(
                "Divident value \"{}\" can't be converted to number".format(
                    divident
                )
            )

        if not divisor or not is_string_number(divisor):
            raise InvalidFpsValue(
                "Divisor value \"{}\" can't be converted to number".format(
                    divident
                )
            )
        divisor_float = float(divisor)
        if divisor_float == 0.0:
            raise InvalidFpsValue("Can't divide by zero")
        return float(divident) / divisor_float

    raise InvalidFpsValue(
        "Value can't be converted to number \"{}\"".format(source_value)
    )


def get_host_ip():
    """Get IP of machine.

    Returns:
        Union[str, None]: IP address of machine or None if could not be
            detected.
    """

    host_name = socket.gethostname()
    try:
        return socket.gethostbyname(host_name)
    except Exception:
        pass

    return None


def get_ftrack_public_url(*args, addon_version, addon_name=None):
    """Url to public path in ftrack addon.

    Args:
        args (tuple[str]): Subpaths in 'public' dir.
        addon_version (str): Version of addon.
        addon_name (Optional[str]): Name of addon. This is for development
            purposes. Default value 'ftrack'.

    Returns:
        str: Url to public file on server in ftrack addon.
    """

    server_url = get_base_url()
    parts = [
        server_url,
        "addons",
        addon_name or "ftrack",
        addon_version,
        "public"
    ]
    parts.extend(args)
    return "/".join(parts)


def get_ftrack_icon_url(icon_name, addon_version, addon_name=None):
    """Helper to get icon url to server.

    The existence of file is not validated.

    Args:
        icon_name (str): Name of icon filename.
        addon_version (str): Version of addon.
        addon_name (Optional[str]): Name of addon. For development purposes.
            Default value 'ftrack'.

    Returns:
        str: Url to icon on server.
    """

    return get_ftrack_public_url(
        "icons", icon_name,
        addon_version=addon_version,
        addon_name=addon_name
    )


def get_service_ftrack_icon_url(
    icon_name, addon_version=None, addon_name=None
):
    """Icon url to server for service process.

    Information about addon version are taken from registered service
    in 'ayon_api'.

    Args:
        icon_name (str): Name of icon filename.
        addon_version (Optional[str]): Version of addon. Version from
            registered service is used if not passed. For development purposes.
        addon_name (Optional[str]): Name of addon. For development purposes.

    Returns:
        str: Url to icon on server.
    """

    return get_ftrack_icon_url(
        icon_name,
        addon_version=addon_version or get_service_addon_version(),
        addon_name=addon_name or get_service_addon_name()
    )