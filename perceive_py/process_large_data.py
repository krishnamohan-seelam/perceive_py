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
import logging
from logging.handlers import RotatingFileHandler


FILE_PATH = "large_data.csv"


# Configure logger
LOG_FILE = "process_large_data.log"
logger = logging.getLogger("process_large_data")
logger.setLevel(logging.DEBUG)

# Create a rotating file handler
handler = RotatingFileHandler(LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=5)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

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
    A decorator that measures and logs the execution time of the decorated function.

    This decorator logs the start time, end time, and the elapsed time of the function
    execution. It also logs the function name and timestamps in a human-readable format.

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
        logger.info(f"Starting '{func.__name__}' at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}")
        value = func(*args, **kwargs)
        end_time = time.perf_counter()
        elapsed_time = end_time - start_time
        logger.info(f"Finished '{func.__name__}' at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}")
        logger.info(f"Elapsed time for '{func.__name__}': {elapsed_time:0.4f} seconds")
        return value

    return wrapper_timer

#  Create sample large DataFrame
@timer
def create_large_dataframe(num_rows=3000, num_cols=10):
    """
    Create a large DataFrame for testing purposes with a row identifier column,
    alternating numeric and string columns.

    This function generates a DataFrame with the specified number of rows and columns.
    The first column is a row identifier, and the remaining columns alternate between
    numeric and string data. The random number generation is seeded for reproducibility.

    Args:
        num_rows (int): Number of rows in the DataFrame.
        num_cols (int): Total number of columns excluding the row identifier.

    Returns:
        pd.DataFrame: A DataFrame containing the generated data with column names
        in the format 'row_id', 'num_col_0', 'str_col_1', ..., alternating between numeric and string columns.
    """
    np.random.seed(0)  # For reproducibility

    # Generate row identifier
    row_ids = np.arange(1, num_rows + 1).reshape(-1, 1)

    # Determine the number of numeric and string columns
    num_numeric_cols = num_cols // 2
    num_string_cols = num_cols - num_numeric_cols

    # Generate numeric data
    numeric_data = np.random.rand(num_rows, num_numeric_cols)
    numeric_columns = [f"num_col_{i}" for i in range(num_numeric_cols)]

    # Generate string data
    string_data = np.random.choice(['A', 'B', 'C', 'D'], size=(num_rows, num_string_cols))
    string_columns = [f"str_col_{i}" for i in range(num_string_cols)]

    # Combine row identifier, numeric, and string data into a single DataFrame
    data = np.hstack((row_ids, numeric_data, string_data))
    columns = ["row_id"] + numeric_columns + string_columns
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

def write_chunk(chunk_id, chunk_df, output_file,write_header=False):
    """
    Writes a DataFrame chunk to a CSV file.
    This function appends the given DataFrame chunk to the specified CSV file. 
    If the file does not exist, it creates a new file and writes the header. 
    In case of any failure during the write operation, an error message is logged.
    Args:
        chunk_id (int): The identifier for the chunk being written.
        chunk_df (pandas.DataFrame): The DataFrame chunk to be written to the file.
        output_file (str): The path to the output CSV file.
    Raises:
        Exception: Catches and logs any exception that occurs during the write operation.
    """
    try:
        mode = "a"  # Always append mode
        header = write_header # Write header only if file is empty

        chunk_df.to_csv(output_file, mode=mode, header=header, index=False)
        logger.info(f"Chunk {chunk_id} written successfully.")

    except Exception as e:
        logger.error(f"Error writing chunk {chunk_id}: {e}")

def write_to_file(output_file, chunks, num_workers=5):
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
            if i == 0:
                write_chunk(i, chunk, output_file,write_header=True)  # Write the first chunk immediately
            else:
                future = executor.submit(write_chunk, i, chunk,output_file,write_header=False)
                futures[future] = (i, chunk)

        # Wait for all futures to complete
        for future in concurrent.futures.as_completed(futures):
            i, chunk = futures[future]
            try:
                future.result()  # Raise exception if the task failed
            except Exception as e:
                logger.error(f"Failed to write chunk {i}: {e}")
                failed_chunks.append((i, chunk))

        # Retry failed chunks
        if failed_chunks:
            logger.warning(f"The following chunks failed to process: {[i for i, _ in failed_chunks]}")
            logger.info("Retrying failed chunks...")
            for i, chunk in failed_chunks:
                try:
                    write_chunk(i, chunk)
                    logger.info(f"Chunk {i} successfully written on retry.")
                except Exception as e:
                    logger.error(f"Retry failed for chunk {i}: {e}")

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
    num_rows=30000
    num_cols=10
    df  = create_large_dataframe(num_rows, num_cols)
    logger.info(f"Created DataFrame with {len(df)} rows and {len(df.columns)} columns.")
   
    
    # Ensure output directory exists
    output_location = Path(output_location)
    output_location.mkdir(parents=True, exist_ok=True)
    output_file = str(output_location / filename)
    
    # Delete the existing file if it exists
    if os.path.exists(output_file):
        os.remove(output_file)
    # Split DataFrame into chunks
    NUM_CHUNKS = 5
    CHUNK_SIZE = len(df) // NUM_CHUNKS
    NUM_WORKERS = 5

    chunks = chunk_generator(df, CHUNK_SIZE, NUM_CHUNKS)


    write_to_file(output_file, chunks,NUM_WORKERS)



if __name__ == "__main__":
    args = get_args()
    main(args.filename, args.output_location)
