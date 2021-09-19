import logging
import typing
from functools import partial
from logging import Logger
from multiprocessing import Pool, Event
from typing import TypeVar, Callable, Union

from cassandra.cluster import Session
from cassandra.query import Statement

from multiprocess_logger import multiprocess_logger
from range_distributor import distribute_by_range

T = TypeVar('T')
C = TypeVar('C')
R = TypeVar('R')


def process_rows_by_range(pool: Pool, n: int,
                          session_supplier: Callable[[], Session],
                          statement_supplier: Callable[[int, int], Statement],
                          row_handler: Callable[[Union[tuple, dict], C, int], C],
                          context_supplier: Callable[[], C] = lambda: None,
                          context_consumer: Callable[[C], R] = lambda c: c,
                          logger: Logger = multiprocess_logger(__file__),
                          log_progress_level: Union[int, str] = logging.DEBUG,
                          log_progress_each: int = None,
                          check_event_each: int = None) -> list[Union[R, Exception]]:
    partial_wrapper = typing.cast(Callable[[int, int, int, Event], T],
                                  partial(_func_wrapper, session_supplier, statement_supplier, row_handler,
                                          context_supplier, context_consumer,
                                          logger, log_progress_level, log_progress_each, check_event_each))
    return distribute_by_range(pool, n, partial_wrapper)


def _func_wrapper(session_supplier: Callable[[], Session],
                  statement_supplier: Callable[[int, int], Statement],
                  row_handler: Callable[[Union[tuple, dict], C, int], C],
                  context_supplier: Callable[[], C],
                  context_consumer: Callable[[C], R],
                  logger: Logger,
                  log_progress_level: Union[int, str],
                  log_progress_each: int,
                  check_event_each: int,
                  index: int, low_inc: int, high_inc: int, event: Event) -> list[Union[R, Exception]]:
    context = context_supplier()
    session = session_supplier()
    statement = statement_supplier(low_inc, high_inc)
    rows = session.execute(statement)

    if not log_progress_each:
        log_progress_each = statement.fetch_size
    if not check_event_each:
        check_event_each = log_progress_each // 10

    log_progress_counter = 0
    check_event_counter = 0
    for i, row in enumerate(rows):
        context = row_handler(row, context, i)

        log_progress_counter += 1
        if log_progress_counter == log_progress_each:
            log_progress_counter = 0
            logger.log(log_progress_level, "%s records processed by %s process with paging state %s",
                       i + 1, index, rows.paging_state.hex())

        check_event_counter += 1
        if check_event_counter == check_event_each:
            check_event_counter = 0
            logger.info("Process % has been stopped", index)
            if event.is_set:
                break

    session.cluster.shutdown()
    return context_consumer(context)
