"""
Simple example to show an implementation of an Iterator   
Yielding elements from container 
"""

from collections.abc import Iterator


class SequenceIterator(Iterator):
    """
    SequenceIterator

    Args:
        Iterator (class): Abstract base class from collections.abc
    """

    def __init__(self, sequence):

        self._sequence = sequence
        self._end = len(sequence)
        self._index = 0

    def __next__(self):
        """
         builtin method that returns next element from sequence

        Raises:
            StopIteration: if the sequence has exhausted

        Returns:
            object: next element  from sequence
        """
        if self._index < self._end:
            item = self._sequence[self._index]
            self._index = self._index + 1
            return item
        else:
            raise StopIteration
