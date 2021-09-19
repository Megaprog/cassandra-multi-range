from multiprocessing import Pool, Event, Manager
from typing import Callable, TypeVar, Union

from token_range import split_long, token_range_inclusive

T = TypeVar('T')


def distribute_by_range(pool: Pool, n: int, func: Callable[[int, int, int, Event], T]) -> list[Union[T, Exception]]:
    splits = split_long(n)
    with Manager() as manager:
        event = manager.Event()
        return pool.starmap(_exception_handler_wrapper, ((i, *token_range_inclusive(splits, i), event, func) for i in range(n)))


def _exception_handler_wrapper(index: int, low_inc: int, high_inc: int, event: Event,
                               func: Callable[[int, int, int, Event], T]):
    try:
        return func(index, low_inc, high_inc, event)
    except Exception as e:
        event.set()
        wrapped_exception = RuntimeError("Unhandled exception in process number {}".format(index))
        wrapped_exception.__cause__ = e
        return wrapped_exception
