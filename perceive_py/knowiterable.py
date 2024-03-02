"""
Simple example to show implementation of an Iterator    
"""

from typing import Iterator
from perceive_py.knowiter import SequenceIterator
from collections.abc import Iterable


class SequenceIterable(Iterable):
    """Sequence Iterable

    Args:
        Iterable (class): Abstract base class from collections.abc
    """

    def __init__(self, sequence):

        self._sequence = sequence

    def __iter__(self) -> Iterator:
        """Creates a Iterator and returns it

        Returns:
            Iterator: Sequence Iterator
        """
        return SequenceIterator(self._sequence)
