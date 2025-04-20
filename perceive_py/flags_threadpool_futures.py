from concurrent import futures
from sequential_downloads import get_flag, save_flag
from typing import Callable
from pathlib import Path
import time

POP20_CC = ("CN IN US ID BR PK NG BD RU JP MX PH VN ET EG DE IR TR CD FR").split()
BASE_URL = "https://www.fluentpython.com/data/flags"
DEST_DIR = Path("downloaded")


def download_one(cc: str):
    image = get_flag(cc)
    save_flag(image, f"{cc}.gif")
    print(cc, end=" ", flush=True)
    return cc


def download_many(cc_list: list[str]) -> int:
    with futures.ThreadPoolExecutor(max_workers=3) as thread_executor:
        to_do: list[futures.Future] = []
        for cc in cc_list:
            future = thread_executor.submit(download_one, cc)
            to_do.append(future)
            print(f"Scheduled for {cc}: {future}")
        for count, future in enumerate(futures.as_completed(to_do), 1):
            res: str = future.result()
            print(f"{future} result: {res!r}")
    return count


def main(downloader: Callable[[list[str]], int]) -> None:
    DEST_DIR.mkdir(exist_ok=True)
    t0 = time.perf_counter()
    count = downloader(POP20_CC)
    elapsed = time.perf_counter() - t0
    print(f"\n{count} downloads in {elapsed:.2f}s")


if __name__ == "__main__":
    main(download_many)
