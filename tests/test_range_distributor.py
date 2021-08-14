from multiprocessing.pool import ThreadPool

from range_distributor import distribute_by_range
from token_range import LONG_MIN_VALUE, LONG_MAX_VALUE


def test_distribute_by_range():
    def validate_range(args):
        index, low_inc, high_inc = args
        if index == 0:
            assert low_inc == LONG_MIN_VALUE
            assert high_inc == -1
        else:
            assert low_inc == 0
            assert high_inc == LONG_MAX_VALUE

        return index

    with ThreadPool(2) as thread_pool:
        result = set(distribute_by_range(thread_pool, 2, validate_range))

    assert result == {0, 1}
