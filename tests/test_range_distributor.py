from multiprocessing import Event
from multiprocessing.pool import ThreadPool

from range_distributor import distribute_by_range
from token_range import LONG_MIN_VALUE, LONG_MAX_VALUE


def test_distribute_by_range():
    def validate_range(index: int, low_inc: int, high_inc: int, event: Event):
        if index == 0:
            assert low_inc == LONG_MIN_VALUE
            assert high_inc == -1
            return "a"
        else:
            assert low_inc == 0
            assert high_inc == LONG_MAX_VALUE
            return "b"

    with ThreadPool(2) as thread_pool:
        result = distribute_by_range(thread_pool, 2, validate_range)

    assert result == ["a", "b"]


def test_distribute_by_range_exception():
    def validate_range(index: int, low_inc: int, high_inc: int, event: Event):
        if index == 0:
            assert low_inc == LONG_MIN_VALUE
            assert high_inc == -1
            event.wait(60)
            return "a"
        else:
            assert low_inc == 0
            assert high_inc == LONG_MAX_VALUE
            raise ValueError("b")

    with ThreadPool(2) as thread_pool:
        result = distribute_by_range(thread_pool, 2, validate_range)

    assert len(result) == 2
    assert result[0] == "a"
    assert isinstance(result[1], ValueError)
    assert result[1].args[0] == "b"
