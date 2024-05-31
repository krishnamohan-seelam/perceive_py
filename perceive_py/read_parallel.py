import multiprocessing as mp
import time
import os
import argparse
from pathlib import Path
import functools
import time


LINE_DELIMITER = "\n"


def timer(func):
    @functools.wraps(func)
    def wrapper_timer(*args, **kwargs):
        tic = time.perf_counter()
        value = func(*args, **kwargs)
        toc = time.perf_counter()
        elapsed_time = toc - tic
        print(f"Elapsed time: {elapsed_time:0.4f} seconds")
        return value

    return wrapper_timer


def process_line(line):
    return line


def process_chunk(file_name, chunk_start, chunk_end):
    chunk_results = []
    with open(file_name, "r") as f:
        # Moving stream position to `chunk_start`
        f.seek(chunk_start)

        # Read and process lines until `chunk_end`
        for line in f:
            chunk_start += len(line)
            if chunk_start > chunk_end:
                break
            chunk_results.append(process_line(line))
    return chunk_results


def is_line_start(file_ctx, position, line_delimiter):
    if position == 0:
        return True
    # check if last character is EOL
    file_ctx.seek(position - 1)
    return file_ctx.read(1) == line_delimiter


def seek_next_line(file_ctx, position):
    file_ctx.seek(position)
    file_ctx.readline()
    return file_ctx.tell()


def read_parallel(filename):
    no_of_cpus = mp.cpu_count()
    file_size = os.path.getsize(filename)
    chunk_size = file_size // no_of_cpus
    chunk_args = []
    chunk_results = []
    print("Chunk size", chunk_size)
    with open(filename, "r") as file_ctx:
        chunk_start = 0
        while chunk_start < file_size:
            chunk_end = min(file_size, chunk_start + chunk_size)
            while not is_line_start(file_ctx, chunk_end, line_delimiter=LINE_DELIMITER):
                chunk_end -= 1
            # edge case if chunk size < line
            if chunk_start == chunk_end:
                chunk_end = seek_next_line(file_ctx, chunk_end)

            chunk_args.append((filename, chunk_start, chunk_end))
            chunk_start = chunk_end
    with mp.Pool(no_of_cpus) as p:
        chunk_results = p.starmap(process_chunk, chunk_args)
    return chunk_results


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("filename", help=" Enter filename")
    parser.add_argument("output_location", help=" Enter output directory")
    args = parser.parse_args()
    return args


def get_output_filename(output_location, filename, partno):

    inp_filename = rf"{filename}"
    print(inp_filename)
    file_path = Path(inp_filename)
    output_filename = file_path.stem + str(partno) + file_path.suffix
    print(output_filename)
    output_path = Path(output_location, output_filename)
    return output_path


@timer
def main(filename, output_location):
    chunk_results = read_parallel(filename)
    for partno, result in enumerate(chunk_results):
        output_filename = get_output_filename(output_location, filename, partno)
        with open(output_filename, "w") as out_ctx:
            for chunk in result:
                out_ctx.write(chunk)
            print("Chunk ", partno, "written to", output_filename)


if __name__ == "__main__":
    args = get_args()
    main(args.filename, args.output_location)
