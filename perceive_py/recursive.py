from typing import Sequence


def recursive_max(inp: Sequence, max=0):
    """
    The function `recursive_max` recursively finds and returns the maximum value in a given sequence.

    :param inp: The `inp` parameter is a sequence (list, tuple, etc.) of elements for which we want to
    find the maximum value using recursion
    :type inp: Sequence
    :param max: The `max` parameter in the `recursive_max` function is used to keep track of the maximum
    value found so far during the recursive process. It is initialized with a default value of 0 when
    the function is called. The function recursively compares each element of the input sequence (`inp`)
    with the, defaults to 0 (optional)
    :return: The function `recursive_max` is returning the maximum value found in the input sequence
    `inp`.
    """

    if len(inp) == 0:
        return max
    if inp[0] > max:
        max = inp[0]
    return recursive_max(inp[1:], max)


def quicksort(inp: Sequence):
    """
    The function `quicksort` implements the quicksort algorithm to sort a sequence of elements.

    :param inp: Sequence
    :type inp: Sequence
    :return: The `quicksort` function is returning a sorted version of the input sequence `inp`. It uses
    the quicksort algorithm to recursively partition the input sequence into elements less than the
    pivot, the pivot itself, and elements greater than the pivot. The function then concatenates the
    sorted results of the recursive calls on the elements less than the pivot and greater than the
    pivot, with the pivot in between,
    """
    if len(inp) < 2:
        return inp
    else:
        pivot = inp[0]
        less_than_pivot = [el for el in inp[1:] if el < pivot]
        greater_than_pivot = [el for el in inp[1:] if el > pivot]
        return quicksort(less_than_pivot) + [pivot] + quicksort(greater_than_pivot)
