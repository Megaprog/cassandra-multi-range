from multiprocessing import Event, Pool
from multiprocessing.pool import ThreadPool

from range_distributor import distribute_by_range
from token_range import LONG_MIN_VALUE, LONG_MAX_VALUE


def test_distribute_by_range():
    with ThreadPool(2) as thread_pool:
        result = distribute_by_range(thread_pool, 2, _validate_range)

    assert result == ["a", "b"]


def test_distribute_by_range_multiprocessing():
    with Pool(2) as pool:
        result = distribute_by_range(pool, 2, _validate_range)

    assert result == ["a", "b"]


def test_distribute_by_range_exception():
    with ThreadPool(2) as thread_pool:
        result = distribute_by_range(thread_pool, 2, _validate_range_exception)

    assert len(result) == 2
    assert result[0] == "a"
    e = result[1]
    assert isinstance(e, ValueError)
    assert e.args[0] == "b"
    assert e.args[2] == 1


def test_distribute_by_range_multiprocessing_exception():
    with Pool(2) as pool:
        result = distribute_by_range(pool, 2, _validate_range_exception)

    assert len(result) == 2
    assert result[0] == "a"
    e = result[1]
    assert isinstance(e, ValueError)
    assert e.args[0] == "b"
    assert e.args[2] == 1


def _validate_range(index: int, low_inc: int, high_inc: int, event: Event):
    if index == 0:
        assert low_inc == LONG_MIN_VALUE
        assert high_inc == -1
        return "a"
    else:
        assert low_inc == 0
        assert high_inc == LONG_MAX_VALUE
        return "b"


def _validate_range_exception(index: int, low_inc: int, high_inc: int, event: Event):
    if index == 0:
        assert low_inc == LONG_MIN_VALUE
        assert high_inc == -1
        event.wait(60)
        return "a"
    else:
        assert low_inc == 0
        assert high_inc == LONG_MAX_VALUE
        raise ValueError("b")

