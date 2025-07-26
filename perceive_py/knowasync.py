import time
from itertools import cycle
import asyncio


async def spin(message: str):
    for ch in cycle(r"\|/-"):
        status = f"\r{ch} {message}"
        print(status, end="", flush=True)
        try:
            await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            break
    blanks = "" * len(status)
    print(f"\r{blanks}\r", end="")


async def slow():
    await asyncio.sleep(5)
    return 3.14


async def supervisor():
    spinner = asyncio.create_task(spin("Thinking!"))
    print("Spinner created:", spinner)
    result = await slow()
    spinner.cancel()
    return result


def main():
    """
    Entry point for the script. Runs the asynchronous supervisor task and prints the result.
    """
    result = asyncio.run(supervisor())
    print("Answer is: ", result)


if __name__ == "__main__":
    main()
