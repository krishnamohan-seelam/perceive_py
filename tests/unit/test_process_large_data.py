
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
    mock_parse_args = mocker.patch("argparse.ArgumentParser.parse_args")
    mock_parse_args.return_value = argparse.Namespace(
        filename="output.csv", output_location="output_dir"
    )
    return mock_parse_args


def test_create_large_dataframe():
    df = create_large_dataframe(300, 10)
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (300, 10)
    assert all(col.startswith("col_") for col in df.columns)

def test_chunk_generator():
    df = pd.DataFrame({"A": range(100)})
    chunk_size = 10
    num_chunks = 10
    chunks = list(chunk_generator(df, chunk_size, num_chunks))
    assert len(chunks) == num_chunks
    for i, chunk in enumerate(chunks):
        assert chunk.equals(df.iloc[i * chunk_size:(i + 1) * chunk_size])

def test_write_chunk(tmp_path):
    output_file = tmp_path / "test_output.csv"
    chunk_df = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})
    write_chunk(0, chunk_df, str(output_file))
    assert output_file.exists()
    written_df = pd.read_csv(output_file)
    pd.testing.assert_frame_equal(chunk_df, written_df)


def test_write_to_file(output_file_fixture, chunks_fixture, mocker):
    mock_write_chunk = mocker.patch("perceive_py.process_large_data.write_chunk")
    write_to_file(str(output_file_fixture), chunks_fixture, num_workers=2)
    assert mock_write_chunk.call_count == len(chunks_fixture)


def test_get_args(mock_args):
    args = get_args()
    assert args.filename == "output.csv"
    assert args.output_location == "output_dir"

def test_create_large_dataframe_default():
    df = create_large_dataframe()
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (30000000, 10)
    assert all(col.startswith("col_") for col in df.columns)
    assert df.isnull().sum().sum() == 0

def test_create_large_dataframe_custom_size():
    num_rows = 1000
    num_cols = 5
    df = create_large_dataframe(num_rows=num_rows, num_cols=num_cols)
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (num_rows, num_cols)
    assert all(col.startswith("col_") for col in df.columns)
    assert df.isnull().sum().sum() == 0

def test_create_large_dataframe_reproducibility():
    df1 = create_large_dataframe(num_rows=100, num_cols=5)
    df2 = create_large_dataframe(num_rows=100, num_cols=5)
    pd.testing.assert_frame_equal(df1, df2)