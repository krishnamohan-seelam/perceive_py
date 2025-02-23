import time
import asyncio


async def main(name):
    print(f"{time.ctime()} Hello, {name}")
    await asyncio.sleep(1.0)
    print(f"{time.ctime()} GoodBye !")


def blocking_io():
    time.sleep(0.5)
    print(f"{time.ctime()} Hello from a thread!")


def run():
    loop = asyncio.get_event_loop()
    task = loop.create_task(main("Krishna"))
    loop.run_in_executor(None, blocking_io)
    loop.run_until_complete(task)

    pending_tasks = asyncio.all_tasks(loop=loop)

    group = asyncio.gather(*pending_tasks, return_exceptions=True)
    loop.run_until_complete(group)
    loop.close()


if __name__ == "__main__":
    run()
