"""Task registry and decorator."""

from functools import wraps

from taskqueue.producer import enqueue_task


task_registry = {}


def task(func):
    """Register task and attach delay method."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    
    def delay(*args, **kwargs):
        priority = kwargs.pop("priority", 1)
        return enqueue_task(func.__name__, args, kwargs, priority)

    wrapper.delay = delay
    task_registry[func.__name__] = wrapper
    return wrapper


