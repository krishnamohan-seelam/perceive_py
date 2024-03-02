from perceive_py.knowiterable import SequenceIterable
from .fixtures import get_sequence, get_empty_sequence
import pytest


def test_sequence_iterable(get_sequence):
    """
    test_sequence_iterable - Asserts the next element of Sequence Iterator
    """
    test_items = [1, 2, 3]
    seq_iter = iter(SequenceIterable(get_sequence))
    assert test_items[0] == next(seq_iter)


def test_stop_iterable(get_empty_sequence):
    """
    test_stop_iterable - Asserts Raises on StopIteration
    """
    seq_iter = iter(SequenceIterable(get_empty_sequence))
    with pytest.raises(StopIteration):
        next(seq_iter)


def test_next_on_Iterable():
    """
    test_next_on_Iterable - Asserts TypeError on an empty sequence
    """
    test_items = []
    seq_iter = SequenceIterable(test_items)
    with pytest.raises(TypeError):
        next(seq_iter)
