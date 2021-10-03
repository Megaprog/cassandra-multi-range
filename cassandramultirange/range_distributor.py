from multiprocessing import Pool, Event, Manager
from typing import Callable, TypeVar, Union

from tblib import pickling_support

from token_range import split_long, token_range_inclusive

T = TypeVar('T')


def distribute_by_range(pool: Pool, n: int, func: Callable[[int, int, int, Event], T]) -> list[Union[T, BaseException]]:
    splits = split_long(n)
    with Manager() as manager:
        event = manager.Event()
        return pool.starmap(_exception_handler_wrapper,
                            ((i, *token_range_inclusive(splits, i), event, func) for i in range(n)))


def _exception_handler_wrapper(process_index: int, low_inc: int, high_inc: int, event: Event,
                               func: Callable[[int, int, int, Event], T]):
    pickling_support.install()
    try:
        return func(process_index, low_inc, high_inc, event)
    except BaseException as e:
        event.set()
        e.args += ("Unhandled exception in process number", process_index)
        return e


def validate_result(result: list[Union[T, BaseException]]) -> list[Union[T, BaseException]]:
    for value in result:
        if isinstance(value, BaseException):
            raise value

    return result
