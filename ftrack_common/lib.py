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
