from perceive_py.knowiter import SequenceIterator
from .fixtures import get_sequence, get_empty_sequence
import pytest


def test_sequence_iter(get_sequence):
    """
    test_sequence_iter - Asserts the next element of Sequence Iterator
    """
    test_items = [1, 2, 3]
    seq_iter = SequenceIterator(get_sequence)
    assert test_items[0] == next(seq_iter)


def test_stop_iteration(get_empty_sequence):
    """
    test_sequence_iter - Asserts Raises on StopIteration
    """
    seq_iter = SequenceIterator(get_empty_sequence)
    with pytest.raises(StopIteration):
        next(seq_iter)
