"""
    Fibanocci Iterator
    Simple iterator example that generates stream of new data elements
"""

from collections.abc import Iterator


class FibanocciIterator(Iterator):
    """
    FibanocciIterator

    Simple iterator example that generates stream of new data elements

    Args:
        Iterator (_type_): _description_
    """

    def __init__(self, stop=10) -> None:
        """
        Initialize FibanocciIterator
        Args:
            stop (int, optional): _description_. Defaults to 10.
        """
        self._stop = stop
        self._start = 0
        self._number = 0
        self._next = 1

    def __next__(self):
        """
         builtin method that returns next element from sequence

        Raises:
            StopIteration: if the sequence has exhausted

        Returns:
            object: next element  from sequence
        """
        if self._start < self._stop:
            result = self._number
            self._number, self._next = self._next, self._number + self._next
            self._start += 1
            return result
        else:
            raise StopIteration
