# -*- coding: utf-8 -*-

import abc
import logging

from .config import CursorPosition
from ..logexception import LogException

logger = logging.getLogger(__name__)


class ConsumerProcessorBase(object):
    @abc.abstractmethod
    def initialize(self, shard):
        raise NotImplementedError('not initialize shard')

    @abc.abstractmethod
    def process(self, log_groups, check_point_tracker):
        raise NotImplementedError('not create method process')

    @abc.abstractmethod
    def shutdown(self, check_point_tracker):
        raise NotImplementedError('not create method shutdown')


class TaskResult(object):
    def __init__(self, task_exception):
        self.task_exception = task_exception

    def get_exception(self):
        return self.task_exception


class ProcessTaskResult(TaskResult):
    def __init__(self, rollback_check_point):
        super(ProcessTaskResult, self).__init__(None)
        self.rollback_check_point = rollback_check_point

    def get_rollback_check_point(self):
        return self.rollback_check_point


class InitTaskResult(TaskResult):
    def __init__(self, cursor, cursor_persistent):
        super(InitTaskResult, self).__init__(None)
        self.cursor = cursor
        self.cursor_persistent = cursor_persistent

    def get_cursor(self):
        return self.cursor

    def is_cursor_persistent(self):
        return self.cursor_persistent


class FetchTaskResult(TaskResult):
    def __init__(self, fetched_log_group_list, cursor):
        super(FetchTaskResult, self).__init__(None)
        self.fetched_log_group_list = fetched_log_group_list
        self.cursor = cursor

    def get_fetched_log_group_list(self):
        return self.fetched_log_group_list

    def get_cursor(self):
        return self.cursor


def consumer_process_task(processor, log_groups, check_point_tracker):
    """
    return TaskResult if failed,
    return ProcessTaskResult if succeed
    :param processor:
    :param log_groups:
    :param check_point_tracker:
    :return:
    """
    try:
        check_point = processor.process(log_groups, check_point_tracker)
        check_point_tracker.flush_check()
    except Exception as e:
        import traceback
        traceback.print_exc()
        return TaskResult(e)
    return ProcessTaskResult(check_point)


def consumer_initialize_task(processor, consumer_client, shard_id, cursor_position, cursor_start_time):
    """
    return TaskResult if failed, or else, return InitTaskResult
    :param processor:
    :param consumer_client:
    :param shard_id:
    :param cursor_position:
    :param cursor_start_time:
    :return:
    """
    try:
        processor.initialize(shard_id)
        is_cursor_persistent = False
        check_point = consumer_client.get_check_point(shard_id)
        if check_point['checkpoint'] and len(check_point['checkpoint']) > 0:
            is_cursor_persistent = True
            cursor = check_point['checkpoint']
        else:
            if cursor_position == CursorPosition.BEGIN_CURSOR:
                cursor = consumer_client.get_begin_cursor(shard_id)
            elif cursor_position == CursorPosition.END_CURSOR:
                cursor = consumer_client.get_end_cursor(shard_id)
            else:
                cursor = consumer_client.get_cursor(shard_id, cursor_start_time)
        return InitTaskResult(cursor, is_cursor_persistent)
    except Exception as e:
        return TaskResult(e)


def consumer_fetch_task(loghub_client_adapter, shard_id, cursor, max_fetch_log_group_size=1000):
    exception = None

    for retry_times in range(3):
        try:
            response = loghub_client_adapter.pull_logs(shard_id, cursor, count=max_fetch_log_group_size)
            fetch_log_group_list = response.get_loggroup_list()
            logger.debug("shard id = " + str(shard_id) + " cursor = " + cursor
                         + " next cursor" + response.get_next_cursor() + " size:" + str(response.get_log_count()))
            next_cursor = response.get_next_cursor()
            if not next_cursor:
                return FetchTaskResult(fetch_log_group_list, cursor)
            else:
                return FetchTaskResult(fetch_log_group_list, next_cursor)
        except LogException as e:
            exception = e
        except Exception as e1:
            logger.error(e1, exc_info=True)
            raise Exception(e1)

        # only retry if the first request get "SLSInvalidCursor" exception
        if retry_times == 0 and isinstance(exception, LogException) \
                and 'invalidcursor' in exception.get_error_code().lower():
            try:
                cursor = loghub_client_adapter.get_end_cursor(shard_id)
            except Exception:
                return TaskResult(exception)
        else:
            break

    return TaskResult(exception)


def consumer_shutdown_task(processor, check_point_tracker):
    """
    :param processor:
    :param check_point_tracker:
    :return:
    """
    exception = None
    try:
        processor.shutdown(check_point_tracker)
    except Exception as e:
        print(e)
        exception = None

    try:
        check_point_tracker.get_check_point()
    except Exception:
        logger.error('Failed to flush check point', exc_info=True)

    return TaskResult(exception)
