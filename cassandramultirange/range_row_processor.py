import logging
import typing
from functools import partial
from logging import Logger
from multiprocessing import Pool, Event
from typing import TypeVar, Callable, Union, Iterable, Optional

from cassandra.cluster import Session, ResultSet

from multiprocess_logger import multiprocess_logger
from range_distributor import distribute_by_range, validate_result

T = TypeVar('T')
C = TypeVar('C')
R = TypeVar('R')


def _dummy_logger():
    return Logger("DUMMY")


def process_query_by_range(n: int,
                           session_supplier: Callable[[], Session],
                           result_set_func: Callable[[Session, int, int], ResultSet],
                           row_handler: Callable[[Union[tuple, dict], C, int], C],
                           context_supplier: Union[C, Callable[[], C]] = None,
                           context_consumer: Callable[[C], R] = None,
                           pool: Pool = None,
                           logger_supplier: Optional[Callable[[], Logger]] = _dummy_logger,
                           log_progress_level: Union[int, str] = logging.DEBUG,
                           log_progress_each: int = 10000,
                           check_event_each: int = None
                           ) -> list[Union[C, R, BaseException]]:
    logger_supplier = partial(multiprocess_logger, __file__) if logger_supplier is _dummy_logger else logger_supplier

    partial_func_wrapper = typing.cast(Callable[[int, int, int, Event], T],
                                       partial(_func_wrapper, session_supplier, result_set_func, row_handler,
                                               context_supplier, context_consumer, logger_supplier,
                                               log_progress_level, log_progress_each, check_event_each))

    if pool:
        return distribute_by_range(pool, n, partial_func_wrapper)
    else:
        with Pool(n) as new_pool:
            return distribute_by_range(new_pool, n, partial_func_wrapper)


def _func_wrapper(session_supplier: Callable[[], Session],
                  result_set_func: Callable[[Session, int, int], ResultSet],
                  row_handler: Callable[[Union[tuple, dict], C, int], C],
                  context_supplier: Union[C, Callable[[], C]],
                  context_consumer: Optional[Callable[[C], R]],
                  logger_supplier: Optional[Callable[[], Logger]],
                  log_progress_level: Union[int, str],
                  log_progress_each: int,
                  check_event_each: Optional[int],
                  index: int, low_inc: int, high_inc: int, event: Event
                  ) -> list[Union[C, R, BaseException]]:
    logger = logger_supplier() if logger_supplier else None
    session = session_supplier()
    rows = result_set_func(session, low_inc, high_inc)

    check_event_each = check_event_each or log_progress_each // 10
    context = context_supplier() if callable(context_supplier) else context_supplier
    log_progress_counter = 0
    check_event_counter = 0
    for i, row in enumerate(rows, 1):
        context = row_handler(row, context, i)

        log_progress_counter += 1
        if log_progress_counter == log_progress_each:
            log_progress_counter = 0
            if logger:
                logger.log(log_progress_level, "%s records processed with paging state %s",
                           i, rows.paging_state.hex())

        check_event_counter += 1
        if check_event_counter == check_event_each:
            check_event_counter = 0
            if event.is_set():
                if logger:
                    logger.info("Process %s has been stopped", index)
                break

    session.cluster.shutdown()
    return context_consumer(context) if context_consumer else context


def process_table_by_range(n: int,
                           session_supplier: Callable[[], Session],
                           table_name: str,
                           partition_key_columns: Union[str, Iterable[str]],
                           row_handler: Callable[[Union[tuple, dict], C, int], C],
                           select_columns: Union[str, Iterable[str]] = "*",
                           context_supplier: Union[C, Callable[[], C]] = None,
                           context_consumer: Callable[[C], R] = None,
                           pool: Pool = None,
                           logger_supplier: Optional[Callable[[], Logger]] = _dummy_logger,
                           log_progress_level: Union[int, str] = logging.DEBUG,
                           log_progress_each: int = 10000,
                           check_event_each: int = None
                           ) -> list[Union[C, R, BaseException]]:
    if isinstance(partition_key_columns, str):
        key_str = partition_key_columns
    else:
        key_str = ".".join(partition_key_columns)

    if isinstance(select_columns, str):
        select_str = select_columns
    else:
        select_str = ".".join(select_columns)

    partial_session_supplier_wrapper = typing.cast(
        Callable[[], Session], partial(_wrapped_session_supplier, session_supplier, table_name, key_str, select_str))

    return process_query_by_range(n, partial_session_supplier_wrapper, _result_set_func, row_handler,
                                  context_supplier, context_consumer, pool, logger_supplier,
                                  log_progress_level, log_progress_each, check_event_each)


def _wrapped_session_supplier(session_supplier: Callable[[], Session], table: str, key: str, select: str) -> Session:
    session = session_supplier()
    session.prepared_select = session.prepare(
        "SELECT {select} FROM {table} WHERE token({key}) >= ? and token({key}) <= ?"
            .format(table=table, key=key, select=select))
    return session


def _result_set_func(session: Session, low_inc: int, high_inc: int) -> ResultSet:
    return session.execute(session.prepared_select, (low_inc, high_inc))


def count_table_by_range(n: int,
                         session_supplier: Callable[[], Session],
                         table_name: str,
                         partition_key_columns: Union[str, Iterable[str]],
                         pool: Pool = None,
                         logger_supplier: Optional[Callable[[], Logger]] = _dummy_logger,
                         log_progress_level: Union[int, str] = logging.DEBUG,
                         log_progress_each: int = 10000,
                         check_event_each: int = None
                         ) -> int:
    result = process_table_by_range(n, session_supplier, table_name, partition_key_columns, _count_row_handler,
                                    "now()", 0, pool=pool, logger_supplier=logger_supplier,
                                    log_progress_level=log_progress_level, log_progress_each=log_progress_each,
                                    check_event_each=check_event_each)

    return sum(validate_result(result))


def _count_row_handler(r: Union[tuple, dict], c: int, i: int) -> int:
    return c + 1
