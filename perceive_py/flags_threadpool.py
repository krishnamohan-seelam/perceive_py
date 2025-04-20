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


def download_many(cc_list: list[str]) -> int:
    with futures.ThreadPoolExecutor() as thread_executor:
        res = thread_executor.map(download_one, cc_list)
    return len(list(res))


def main(downloader: Callable[[list[str]], int]) -> None:
    DEST_DIR.mkdir(exist_ok=True)
    t0 = time.perf_counter()
    count = downloader(POP20_CC)
    elapsed = time.perf_counter() - t0
    print(f"\n{count} downloads in {elapsed:.2f}s")


if __name__ == "__main__":
    main(download_many)
