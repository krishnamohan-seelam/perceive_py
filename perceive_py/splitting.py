import sys
from itertools import islice, zip_longest

if sys.version_info >= (3, 12):
    from itertools import batched
else:
    try:
        from more_itertools import batched
    except ImportError:

        def batched(iterable, chunk_size):
            iterator = iter(iterable)
            while chunk := tuple(islice(iterator, chunk_size)):
                yield chunk


def batched_functional(iterable, chunk_size):
    iterator = iter(iterable)
    return iter(lambda: tuple(islice(iterator, chunk_size)), tuple())


def batched_padded(iterable, chunk_size, fillvalue=None):
    return zip_longest(*[iter(iterable)] * chunk_size, fillvalue=fillvalue)


def batched_sequence(sequence, chunk_size):
    for index in range(0, len(sequence), chunk_size):
        yield sequence[index : index + chunk_size]
