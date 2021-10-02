from multiprocessing import Pool, Event, Manager
from typing import Callable, TypeVar, Union

from tblib import pickling_support

from token_range import split_long, token_range_inclusive

T = TypeVar('T')


def distribute_by_range(pool: Pool, n: int, func: Callable[[int, int, int, Event], T]) -> list[Union[T, Exception]]:
    splits = split_long(n)
    with Manager() as manager:
        event = manager.Event()
        return pool.starmap(_exception_handler_wrapper,
                            ((i, *token_range_inclusive(splits, i), event, func) for i in range(n)))


def _exception_handler_wrapper(index: int, low_inc: int, high_inc: int, event: Event,
                               func: Callable[[int, int, int, Event], T]):
    pickling_support.install()
    try:
        return func(index, low_inc, high_inc, event)
    except Exception as e:
        event.set()
        return ExceptionPickleProxy(e, index)


class ExceptionPickleProxy(Exception):
    def __init__(self, e: BaseException, process_index: int):
        super().__init__(*e.args, "Unhandled exception in process number", process_index)
        self.__cause__ = e
        self.process_index = process_index

    def __reduce__(self):
        return _create_exception_instance, (self.__cause__.__class__, self.args, self.__cause__.__traceback__)


def _create_exception_instance(constructor, args, traceback) -> BaseException:
    return constructor(args).with_traceback(traceback)
