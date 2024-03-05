# -*- coding: utf-8 -*-

import abc
import logging

from .config import CursorPosition
from ..logexception import LogException
import time
import six
import sys

logger = logging.getLogger(__name__)


class ConsumerProcessorBase(object):
    def __init__(self):
        self.shard_id = -1
        self.last_check_time = 0
        self.checkpoint_timeout = 3

    def save_checkpoint(self, tracker, force=False):
        current_time = time.time()
        if force or current_time - self.last_check_time > self.checkpoint_timeout:
            try:
                self.last_check_time = current_time
                tracker.save_check_point(True)
            except Exception as ex:
                logger.warning(
                    "Fail to store checkpoint for shard {0}, error: {1}".format(self.shard_id, ex))
        else:
            try:
                tracker.save_check_point(False)
            except Exception as ex:
                logger.warning(
                    "Fail to store checkpoint for shard {0}, error: {1}".format(self.shard_id, ex))

    def initialize(self, shard):
        self.shard_id = shard

    @abc.abstractmethod
    def process(self, log_groups, check_point_tracker):
        raise NotImplementedError('not create method process')

    def shutdown(self, check_point_tracker):
        consumer_client = check_point_tracker.consumer_group_client
        _id = '/'.join([
            consumer_client.mproject, consumer_client.mlogstore,
            consumer_client.mconsumer_group, consumer_client.mconsumer,
            str(self.shard_id)
        ])
        logger.info("[%s]ConsumerProcesser is shutdown, shard id: %s", _id,
                    self.shard_id)
        self.save_checkpoint(check_point_tracker, force=True)


class ConsumerProcessorAdaptor(ConsumerProcessorBase):
    def __init__(self, func):
        super(ConsumerProcessorAdaptor, self).__init__()
        self.func = func

    def process(self, log_groups, check_point_tracker):
        ret = self.func(self.shard_id, log_groups)
        if isinstance(ret, bool) and not ret:
            return  # do not save checkpoint when getting False

        self.save_checkpoint(check_point_tracker)


class TaskResult(object):
    def __init__(self, task_exception):
        self.task_exception = task_exception
        self._exc_info = None
        if six.PY2 and task_exception is not None:
            self._exc_info = sys.exc_info()

    def get_exception(self):
        return self.task_exception

    @property
    def exc_info(self):
        if six.PY3:
            return self.task_exception
        else:
            return self._exc_info


class ProcessTaskResult(TaskResult):
    def __init__(self, rollback_check_point):
        super(ProcessTaskResult, self).__init__(None)
        self.rollback_check_point = rollback_check_point

    def get_rollback_check_point(self):
        return self.rollback_check_point


class InitTaskResult(TaskResult):
    def __init__(self, cursor, cursor_persistent, end_cursor):
        super(InitTaskResult, self).__init__(None)
        self.cursor = cursor
        self.cursor_persistent = cursor_persistent
        self._end_cursor = end_cursor

    def get_cursor(self):
        return self.cursor

    @property
    def end_cursor(self):
        return self._end_cursor

    def is_cursor_persistent(self):
        return self.cursor_persistent


class FetchTaskResult(TaskResult):
    def __init__(self, fetched_log_group_list, cursor, raw_size , raw_size_before_query, raw_log_group_count_before_query):
        super(FetchTaskResult, self).__init__(None)
        self.fetched_log_group_list = fetched_log_group_list
        self.cursor = cursor
        self.raw_size = raw_size
        self.raw_size_before_query = raw_size_before_query
        self.raw_log_group_count_before_query = raw_log_group_count_before_query

    def get_fetched_log_group_list(self):
        return self.fetched_log_group_list

    def get_cursor(self):
        return self.cursor

    def get_raw_size(self):
        return self.raw_size

    def get_raw_size_before_query(self):
        return self.raw_size_before_query

    def get_raw_log_group_count_before_query(self):
        return self.raw_log_group_count_before_query


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
        return TaskResult(e)
    return ProcessTaskResult(check_point)


def consumer_initialize_task(processor, consumer_client, shard_id, cursor_position, cursor_start_time, cursor_end_time=None):
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

        end_cursor = None
        if cursor_end_time is not None:
            end_cursor = consumer_client.get_cursor(shard_id, cursor_end_time)

        return InitTaskResult(cursor, is_cursor_persistent, end_cursor)
    except Exception as e:
        return TaskResult(e)


def consumer_fetch_task(loghub_client_adapter, shard_id, cursor, max_fetch_log_group_size=1000, end_cursor=None, query=None):
    exception = None

    for retry_times in range(3):
        try:
            response = loghub_client_adapter.pull_logs(shard_id, cursor, count=max_fetch_log_group_size, end_cursor=end_cursor, query=query)
            fetch_log_group_list = response.get_loggroup_list()
            next_cursor = response.get_next_cursor()
            raw_size = response.get_raw_size()
            raw_size_before_query = 0
            raw_log_group_count_before_query = 0
            if query:
                raw_size_before_query = max(response.get_raw_size_before_query(), 0)
                raw_log_group_count_before_query = max(response.get_raw_log_group_count_before_query(), 0)
            logger.debug("shard id = %s cursor = %s next cursor = %s size: %s",
                         shard_id, cursor, next_cursor,
                         response.get_log_count())
            if not next_cursor:
                return FetchTaskResult(fetch_log_group_list, cursor, raw_size, raw_size_before_query, raw_log_group_count_before_query)
            else:
                return FetchTaskResult(fetch_log_group_list, next_cursor, raw_size, raw_size_before_query, raw_log_group_count_before_query)
        except LogException as e:
            exception = e
            if exception.get_resp_status() == 403:
                time.sleep(5)
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
