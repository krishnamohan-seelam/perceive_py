"""
This module contains unit tests for the `process_large_data` module in the `perceive_py` package.

The tests cover the following functionalities:
- Creating large DataFrames with specific properties.
- Generating chunks from a DataFrame.
- Writing chunks to a file.
- Writing multiple chunks to a file using multiprocessing.
- Parsing command-line arguments.

Fixtures:
- `output_file_fixture`: Provides a temporary file path for testing file output.
- `chunks_fixture`: Provides sample DataFrame chunks for testing.
- `mock_args`: Mocks command-line arguments for testing argument parsing.

Each test ensures the correctness and expected behavior of the corresponding function in the `process_large_data` module.
"""

import pandas as pd
import os
from pathlib import Path
import argparse
import pytest
from perceive_py.process_large_data import (
    get_args,
    create_large_dataframe,
    chunk_generator,
    write_chunk,
    write_to_file,
)



@pytest.fixture
def output_file_fixture(tmp_path):
    return tmp_path / "test_output.csv"

@pytest.fixture
def chunks_fixture():
    return [pd.DataFrame({"A": [1, 2, 3]}), pd.DataFrame({"A": [4, 5, 6]})]

@pytest.fixture
def mock_args(mocker):
    """
    Mocks the `parse_args` method of `argparse.ArgumentParser` to simulate command-line arguments.

    Args:
        mocker: A pytest-mock fixture used to patch objects during testing.

    Returns:
        MagicMock: A mocked version of `argparse.ArgumentParser.parse_args` that returns a predefined
        `argparse.Namespace` object with `filename` set to "output.csv" and `output_location` set to "output_dir".
    """
    mock_parse_args = mocker.patch("argparse.ArgumentParser.parse_args")
    mock_parse_args.return_value = argparse.Namespace(
        filename="output.csv", output_location="output_dir"
    )
    return mock_parse_args


def test_create_large_dataframe():
    """
    Test the `create_large_dataframe` function to ensure it generates a DataFrame
    with the expected structure and properties.

    The test verifies:
    - The returned object is an instance of `pd.DataFrame`.
    - The shape of the DataFrame matches the expected dimensions (300 rows, 11 columns).
    - All column names (except the first column) start with either "num_col_" or "str_col_".

    This test assumes the `create_large_dataframe` function takes two arguments:
    - `rows`: Number of rows in the DataFrame.
    - `cols`: Number of numeric and string columns to generate.
    """
    df = create_large_dataframe(300, 10)
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (300, 11)
    assert all(col.startswith("num_col_") or col.startswith("str_col_") for col in df.columns[1:])

def test_chunk_generator():
    """
    Tests the `chunk_generator` function to ensure it correctly divides a DataFrame into chunks.

    This test creates a DataFrame with 100 rows and splits it into 10 chunks, each containing 10 rows.
    It verifies that the number of chunks generated matches the expected number and that each chunk
    contains the correct subset of rows from the original DataFrame.

    Assertions:
    - The number of chunks generated matches the expected `num_chunks`.
    - Each chunk is equal to the corresponding slice of the original DataFrame.

    Dependencies:
    - pandas as pd
    """
    df = pd.DataFrame({"A": range(100)})
    chunk_size = 10
    num_chunks = 10
    chunks = list(chunk_generator(df, chunk_size, num_chunks))
    assert len(chunks) == num_chunks
    for i, chunk in enumerate(chunks):
        assert chunk.equals(df.iloc[i * chunk_size:(i + 1) * chunk_size])

def test_write_chunk(tmp_path):
    """
    Test the `write_chunk` function to ensure it writes a DataFrame to a CSV file correctly.

    This test verifies the following:
    - The output file is created at the specified path.
    - The contents of the written file match the original DataFrame.

    Args:
        tmp_path (pathlib.Path): A temporary directory provided by pytest for creating test files.

    Raises:
        AssertionError: If the output file does not exist or the contents of the file do not match the original DataFrame.
    """
    output_file = tmp_path / "test_output.csv"
    chunk_df = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})
    write_chunk(0, chunk_df, str(output_file), write_header=True)
    assert output_file.exists()
    written_df = pd.read_csv(output_file)
    pd.testing.assert_frame_equal(chunk_df, written_df)


def test_write_to_file(output_file_fixture, chunks_fixture, mocker):
    """
    Test the `write_to_file` function to ensure it writes data chunks to a file correctly.

    Args:
        output_file_fixture (str): Fixture providing the path to the output file.
        chunks_fixture (list): Fixture providing the list of data chunks to be written.
        mocker (pytest_mock.MockerFixture): Pytest mocker fixture used to patch dependencies.

    Test Behavior:
        - Mocks the `write_chunk` function to track its call count.
        - Calls `write_to_file` with the provided output file path, data chunks, and number of workers.
        - Asserts that the `write_chunk` function is called the expected number of times, equal to the length of `chunks_fixture`.
    """
    mock_write_chunk = mocker.patch("perceive_py.process_large_data.write_chunk")
    write_to_file(str(output_file_fixture), chunks_fixture, num_workers=2)
    assert mock_write_chunk.call_count == len(chunks_fixture)


def test_get_args(mock_args):
    """
    Unit test for the `get_args` function.

    This test verifies that the `get_args` function correctly retrieves
    and parses command-line arguments. It checks that the returned `args`
    object contains the expected values for `filename` and `output_location`.

    Args:
        mock_args: A mock object or fixture simulating command-line arguments.

    Assertions:
        - Ensures `args.filename` is equal to "output.csv".
        - Ensures `args.output_location` is equal to "output_dir".
    """
    args = get_args()
    assert args.filename == "output.csv"
    assert args.output_location == "output_dir"

def test_create_large_dataframe_default():
    """
    Test the `create_large_dataframe` function with default parameters.

    This test verifies that the function correctly creates a DataFrame with the specified
    number of rows and columns, including the expected column naming convention and
    ensuring no null values are present.

    Assertions:
    - The returned object is an instance of `pd.DataFrame`.
    - The shape of the DataFrame matches the expected dimensions (30 rows, 11 columns).
    - All column names (except the first column) start with either "num_col_" or "str_col_".
    - The DataFrame contains no null values.
    """
    df = create_large_dataframe(num_rows=30, num_cols=10)
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (30, 11)
    assert all(col.startswith("num_col_") or col.startswith("str_col_") for col in df.columns[1:])
    assert df.isnull().sum().sum() == 0

def test_create_large_dataframe_custom_size():
    """
    Test the creation of a large DataFrame with custom size.

    This test verifies that the `create_large_dataframe` function generates a DataFrame
    with the specified number of rows and columns, and ensures the following:
    - The returned object is a pandas DataFrame.
    - The shape of the DataFrame matches the expected dimensions, with an additional column.
    - All column names follow the expected naming convention, starting with "num_col_" or "str_col_".
    - The DataFrame contains no null values.

    Raises:
        AssertionError: If any of the conditions are not met.
    """
    num_rows = 1000
    num_cols = 5
    df = create_large_dataframe(num_rows=num_rows, num_cols=num_cols)
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (num_rows, num_cols + 1)
    assert all(col.startswith("num_col_") or col.startswith("str_col_") for col in df.columns[1:])
    assert df.isnull().sum().sum() == 0

def test_create_large_dataframe_reproducibility():
    """
    Test the reproducibility of the `create_large_dataframe` function.

    This test ensures that calling `create_large_dataframe` with the same parameters
    produces identical DataFrames. It verifies that the function is deterministic
    and consistent in its output.

    Assertions:
    - The two DataFrames created with identical parameters are equal.

    Parameters:
    None

    Returns:
    None
    """
    df1 = create_large_dataframe(num_rows=100, num_cols=5)
    df2 = create_large_dataframe(num_rows=100, num_cols=5)
    pd.testing.assert_frame_equal(df1, df2)