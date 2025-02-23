from functools import wraps


class ControlledException(Exception):
    pass


DEFAULT_LIMIT = 3


def with_retry(limit=DEFAULT_LIMIT, allow_exceptions=None):
    allowed_exceptions = allowed_exceptions or (ControlledException,)

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
