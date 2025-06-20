import numpy as np
import pandas as pd
import concurrent.futures
import os
import time
import os
import argparse
from pathlib import Path
import functools
import time


FILE_PATH = "large_data.csv"


def get_args():
    """
    Parses command-line arguments for the script.

    Returns:
        argparse.Namespace: An object containing the following attributes:
            - filename (str): The name of the input file provided via the --filename argument.
            - output_location (str): The directory path for the output provided via the --output_location argument.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--filename", help=" Enter filename")
    parser.add_argument("--output_location", help=" Enter output directory")
    args = parser.parse_args()
    return args

def timer(func):
    """
    A decorator that measures and prints the execution time of the decorated function.

    This decorator logs the start time, end time, and the elapsed time of the function
    execution. It also prints the function name and timestamps in a human-readable format.

    Args:
        func (Callable): The function to be wrapped and timed.

    Returns:
        Callable: The wrapped function with timing functionality.

    Example:
        @timer
        def example_function():
            # Function logic here
            pass
    """
    @functools.wraps(func)
    def wrapper_timer(*args, **kwargs):
        start_time = time.perf_counter()
        print(f"Starting '{func.__name__}' at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}")
        value = func(*args, **kwargs)
        end_time = time.perf_counter()
        elapsed_time = end_time - start_time
        print(f"Finished '{func.__name__}' at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}")
        print(f"Elapsed time for '{func.__name__}': {elapsed_time:0.4f} seconds")
        return value

    return wrapper_timer

#  Create sample large DataFrame
@timer
def create_large_dataframe(num_rows=30000000, num_cols=10):
    """
    Create a large DataFrame for testing purposes.

    This function generates a DataFrame with 30 million rows and 10 columns,
    filled with random floating-point numbers between 0 and 1. The random
    number generation is seeded for reproducibility.

    Returns:
        pd.DataFrame: A DataFrame containing the generated data with column
        names in the format 'col_0', 'col_1', ..., 'col_9'.
    """
    """Create a large DataFrame for testing purposes."""
    np.random.seed(0)  # For reproducibility

    data = np.random.rand(num_rows, num_cols)
    columns = [f"col_{i}" for i in range(num_cols)]
    df = pd.DataFrame(data, columns=columns)
    return df


# Split DataFrame into chunks
def chunk_generator(df, chunk_size, num_chunks):
    """
    Generates chunks of a DataFrame based on the specified chunk size and number of chunks.

    Args:
        df (pandas.DataFrame): The DataFrame to be divided into chunks.
        chunk_size (int): The number of rows in each chunk.
        num_chunks (int): The total number of chunks to generate.

    Yields:
        pandas.DataFrame: A chunk of the original DataFrame containing `chunk_size` rows.

    Example:
        >>> for chunk in chunk_generator(df, chunk_size=100, num_chunks=10):
        ...     print(chunk)
    """
    for i in range(num_chunks):
        yield df.iloc[i * chunk_size:(i + 1) * chunk_size]

def write_chunk(chunk_id, chunk_df,output_file):
    """
    Writes a DataFrame chunk to a CSV file.
    This function appends the given DataFrame chunk to the specified CSV file. 
    If the file does not exist, it creates a new file and writes the header. 
    In case of any failure during the write operation, an error message is printed.
    Args:
        chunk_id (int): The identifier for the chunk being written.
        chunk_df (pandas.DataFrame): The DataFrame chunk to be written to the file.
        output_file (str): The path to the output CSV file.
    Raises:
        Exception: Catches and prints any exception that occurs during the write operation.
    """
    """Writes a chunk to a CSV file and handles failures."""
    try:
        mode = "a" if os.path.exists(output_file) else "w"
        header = not os.path.exists(output_file)  # Write header only for first chunk

        chunk_df.to_csv(output_file, mode=mode, header=header, index=False)
        print(f"Chunk {chunk_id} written successfully.")    
      

    except Exception as e:
        print(f"Error writing chunk {chunk_id}: {e}")

def write_to_file(output_file, chunks, num_workers=6):
    """
    Writes data chunks to a file using multithreading for parallel processing.

    Args:
        output_file (str): The path to the output file where the chunks will be written.
        chunks (list): A list of data chunks to be written to the file.
        num_workers (int, optional): The number of worker threads to use for parallel processing. Defaults to 6.

    Returns:
        None

    This function processes the given data chunks in parallel using a ThreadPoolExecutor. 
    If any chunk fails to write, it retries writing the failed chunks sequentially. 
    Errors during both the initial write and retry attempts are logged to the console.
    """

    failed_chunks = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = {}
        for i, chunk in enumerate(chunks):
            future = executor.submit(write_chunk, i, chunk,output_file)
            futures[future] = (i, chunk)

        # Wait for all futures to complete
        for future in concurrent.futures.as_completed(futures):
            i, chunk = futures[future]
            try:
                future.result()  # Raise exception if the task failed
            except Exception as e:
                print(f"Failed to write chunk {i}: {e}")
                failed_chunks.append((i, chunk))

        # Retry failed chunks
        if failed_chunks:
            print(f"The following chunks failed to process: {[i for i, _ in failed_chunks]}")
            print("Retrying failed chunks...")
            for i, chunk in failed_chunks:
                try:
                    write_chunk(i, chunk)
                    print(f"Chunk {i} successfully written on retry.")
                except Exception as e:
                    print(f"Retry failed for chunk {i}: {e}")

@timer
def main(filename, output_location):
    """
    Main function to process a large DataFrame, split it into chunks, and write the chunks to an output file.

    Args:
        filename (str): The name of the output file where the processed data will be saved.
        output_location (str or Path): The directory where the output file will be stored. 
                                       If it does not exist, it will be created.

    Returns:
        None
    """
    num_rows=30000000
    num_cols=10
    df  = create_large_dataframe(num_rows, num_cols)
    # Ensure output directory exists
    output_location = Path(output_location)
    output_location.mkdir(parents=True, exist_ok=True)
    output_file = str(output_location / filename)
    # Split DataFrame into chunks
    NUM_CHUNKS = 10
    CHUNK_SIZE = len(df) // NUM_CHUNKS


    chunks = chunk_generator(df, CHUNK_SIZE, NUM_CHUNKS)


    write_to_file(output_file, chunks)



if __name__ == "__main__":
    args = get_args()
    main(args.filename, args.output_location)
