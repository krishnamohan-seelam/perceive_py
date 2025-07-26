import time
from itertools import cycle
from threading import Thread, Event


def spin(message: str, done: Event):
    """
    Spins a loading animation in the console until the `done` event is set.

    Args:
        message (str): The message to display alongside the spinner.
        done (Event): A threading event to signal when to stop the spinner.
    """
    for ch in cycle(r"\|/-"):
        status = f"\r{ch} {message}"
        print(status, end="", flush=True)
        if done.wait(0.1):
            break
    blanks = "" * len(status)
    print(f"\r{blanks}\r", end="")


def slow():
    """
    Simulates a slow operation by sleeping for 5 seconds.

    Returns:
        float: A dummy result value.
    """
    time.sleep(5)
    return 3.14


def supervisor():
    """
    Supervises the spinner and slow operation, ensuring proper synchronization.

    Returns:
        float: The result of the slow operation.
    """
    done = Event()
    spinner = Thread(target=spin, args=("Thinking!", done))
    print("Spinner created:", spinner)
    spinner.start()
    result = slow()
    done.set()
    spinner.join()
    return result


def main():
    """
    Entry point for the script. Runs the supervisor and prints the result.
    """
    result = supervisor()
    print("Answer is: ", result)


if __name__ == "__main__":
    main()
