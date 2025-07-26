from functools import wraps


class ControlledException(Exception):
    pass


DEFAULT_LIMIT = 3


def with_retry(limit=DEFAULT_LIMIT, allow_exceptions=None):
    """
    A decorator to retry a function multiple times in case of exceptions.

    Args:
        limit (int): The maximum number of retry attempts.
        allow_exceptions (tuple): A tuple of exception classes that are allowed for retry.

    Returns:
        Callable: A decorator that retries the function upon encountering allowed exceptions.
    """
    allowed_exceptions = allow_exceptions or (ControlledException,)

    def retry(func):
        @wraps(func)
        def wrapped(*args, **kwargs):
            for _ in range(limit):
                try:
                    result = func(*args, **kwargs)
                    return result
                except allowed_exceptions as ae:
                    last_raised = ae
            raise last_raised

        return wrapped

    return retry
