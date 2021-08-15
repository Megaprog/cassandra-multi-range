from multiprocessing import Pool, Event
from typing import Callable, TypeVar, Iterator, Union

from token_range import split_long, token_range_inclusive

T = TypeVar('T')


def distribute_by_range(pool: Pool, n: int, func: Callable[[int, int, int, Event], T]) -> list[Union[T, Exception]]:
    splits = split_long(n)
    event = Event()

    def wrapper(index: int, low_inc: int, high_inc: int):
        try:
            return func(index, low_inc, high_inc, event)
        except Exception as e:
            event.set()
            return e

    return pool.starmap(wrapper, ((i, *token_range_inclusive(splits, i)) for i in range(n)))
