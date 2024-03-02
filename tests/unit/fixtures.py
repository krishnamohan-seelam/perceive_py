import pytest


@pytest.fixture
def get_sequence():
    return [1, 2, 3]


@pytest.fixture
def get_empty_sequence():
    return []
