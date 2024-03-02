"""
    Unit test case for Fibanocci Iterator
"""

from perceive_py.fibanocci_iter import FibanocciIterator
import pytest


def test_stop_iteration():
    """
    test_stop_iteration - Asserts Raises on StopIteration
    """

    fib_iter = FibanocciIterator(0)
    with pytest.raises(StopIteration):
        next(fib_iter)


def test_fibanocci_iter():
    fib_iter = FibanocciIterator(6)
    expected = [0, 1, 1, 2, 3, 5]
    result = list(fib_iter)
    assert result == expected
