import time
from itertools import cycle
from multiprocessing import Process, Event, synchronize


def spin(message: str, done: synchronize.Event):
    for ch in cycle(r"\|/-"):
        status = f"\r{ch} {message}"
        print(status, end="", flush=True)
        if done.wait(0.1):
            break
    blanks = "" * len(status)
    print(f"\r{blanks}\r", end="")


def slow():
    time.sleep(5)
    return 3.14


def supervisor():
    done = Event()
    spinner = Process(target=spin, args=("Thinking!", done))
    print("Spinner created:", spinner)
    spinner.start()
    result = slow()
    done.set()
    spinner.join()
    return result


def main():
    result = supervisor()
    print("Answer is: ", result)


if __name__ == "__main__":
    main()
