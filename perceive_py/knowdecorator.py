import time
import functools


def timer(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        time.sleep(3)
        result = func(*args, **kwargs)
        stop_time = time.perf_counter()
        run_time = stop_time - start_time
        print(f"Execution Time of {func.__name__} is {stop_time-start_time} seconds")
        return result

    return wrapper


def no_decorator():
    print("Showing output using timer wrapper(alias decorator) without pie syntax")


@timer
def show_output():
    print("Showing output using timer decorator")


no_decorator = timer(no_decorator)


def main():

    print("calling decorator")
    show_output()
    no_decorator()


if __name__ == "__main__":
    main()
