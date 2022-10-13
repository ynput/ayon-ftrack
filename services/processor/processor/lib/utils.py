import ayclient


def get_addon_resource_url(*args):
    # server_url = ayclient.config.server_url
    server_url = "https://dev.openpype.cloud"
    parts = [
        server_url,
        "addons",
        ayclient.addon_name,
        ayclient.addon_version,
        "public"
    ]
    parts.extend(args)
    return "/".join(parts)


def get_icon_url(icon_name):
    return get_addon_resource_url("icons", icon_name)


def create_chunks(iterable, chunk_size=None):
    """Separate iterable into multiple chunks by size.

    Args:
        iterable (Iterable): Object that will be separated into chunks.
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
