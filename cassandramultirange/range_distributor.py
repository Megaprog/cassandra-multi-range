from multiprocessing import Pool
from typing import Callable, TypeVar, Iterator

from token_range import split_long, token_range_inclusive

T = TypeVar('T')


def distribute_by_range(pool: Pool, n: int, func: Callable[[tuple[int, int, int]], int]) -> Iterator[int]:
    splits = split_long(n)
    return pool.imap_unordered(func, ((i, *token_range_inclusive(splits, i)) for i in range(n)))
