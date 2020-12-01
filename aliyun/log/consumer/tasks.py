# -*- coding: utf-8 -*-

import abc
import logging

from .config import CursorPosition
from ..util import PrefixLoggerAdapter
from ..logexception import LogException
import time
import six
import sys
import traceback
from .config import PRE_ALLOCATED_BYTES
logger = logging.getLogger(__name__)


class ConsumerProcessorBase(object):
    def __init__(self):
        self.shard_id = -1
        self.last_check_time = 0
        self.checkpoint_timeout = 3
        self.logger = logging.getLogger(__name__)

    def save_checkpoint(self, tracker, force=False):
        current_time = time.time()
        if force or current_time - self.last_check_time > self.checkpoint_timeout:
            try:
                self.last_check_time = current_time
                tracker.save_check_point(True)
            except Exception as ex:
                self.logger.warning("Fail to store checkpoint, error: %s", ex, extra={"event_id": "consumer_processor:process:fail_save_checkpoint", "reason": str(ex), "trace_back":traceback.format_exc()})
        else:
            try:
                tracker.save_check_point(False)
            except Exception as ex:
                self.logger.warning("Fail to store checkpoint, error: %s", ex, extra={"event_id": "consumer_processor:process:fail_save_checkpoint", "reason": str(ex), "trace_back":traceback.format_exc()})

    def save_metric_log(self,tracker):
        tracker.flush_check_metric()

    def initialize(self, shard):
        self.shard_id = shard

    def initialized_ex(self, consumer_client, shard):
        self.initialize(shard)
        log_prefix = '/'.join([consumer_client.mproject, consumer_client.mlogstore,
            consumer_client.mconsumer_group, consumer_client.mconsumer])

        extra = {
            "etl_context": """{
            "project": "%s", 
            "logstore": "%s", 
            "consumer_group": "%s", 
            "consumer": "%s",
            "shard_id": "%s"} """ % (consumer_client.mproject,
                                          consumer_client.mlogstore,
                                          consumer_client.mconsumer_group,
                                          consumer_client.mconsumer,
                                          shard)
        }

        self.logger = PrefixLoggerAdapter("[{0}/{1}]".format(log_prefix, shard), extra, logging.getLogger(__name__), {})

    @abc.abstractmethod
    def process(self, log_groups, check_point_tracker,fetched_log_raw_data_sizes):
        raise NotImplementedError('not create method process')

    def shutdown(self, check_point_tracker):
        consumer_client = check_point_tracker.consumer_group_client
        _id = '/'.join([
            consumer_client.mproject, consumer_client.mlogstore,
            consumer_client.mconsumer_group, consumer_client.mconsumer,
            str(self.shard_id)
        ])
        self.logger.debug("ConsumerProcesser is shutdown", extra={"event_id": "consumer_processor:shutdown:enter"})


class ConsumerProcessorAdaptor(ConsumerProcessorBase):
    def __init__(self, func):
        super(ConsumerProcessorAdaptor, self).__init__()
        self.func = func

    def process(self, log_groups, check_point_tracker,fetched_log_raw_data_sizes):
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
    def __init__(self, fetched_log_group_list, cursor, fetched_raw_data_sizes):
        super(FetchTaskResult, self).__init__(None)
        self.fetched_log_group_list = fetched_log_group_list
        self.cursor = cursor
        self.fetched_raw_data_sizes = fetched_raw_data_sizes

    def get_fetched_log_group_list(self):
        return self.fetched_log_group_list

    def get_cursor(self):
        return self.cursor

    def get_fetched_raw_data_sizes(self):
        return self.fetched_raw_data_sizes


def consumer_process_task(processor, log_groups, check_point_tracker,fetched_log_raw_data_sizes, shard,
                          resource_barrier=None):
    """
    return TaskResult if failed,
    return ProcessTaskResult if succeed
    :param processor:
    :param log_groups:
    :param check_point_tracker:
    :param fetched_log_raw_data_sizes:
    :return:
    """
    try:
        check_point = processor.process(log_groups, check_point_tracker, fetched_log_raw_data_sizes)
    except Exception as e:
        resource_barrier.release(shard, fetched_log_raw_data_sizes)
        return TaskResult(e)
    resource_barrier.release(shard, fetched_log_raw_data_sizes)
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
        if hasattr(processor, 'initialized_ex'):
            processor.initialized_ex(consumer_client, shard_id)
        else:
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


def consumer_fetch_task(loghub_client_adapter, shard_id, cursor, max_fetch_log_group_size=1000, end_cursor=None,
                        min_batch_fetch_bytes=-1):
    exception = None

    for retry_times in range(3):
        try:
            response = loghub_client_adapter.pull_logs(shard_id, cursor, count=max_fetch_log_group_size, end_cursor=end_cursor)
            fetch_log_group_list = response.get_loggroup_list()
            next_cursor = response.get_next_cursor()
            log_raw_size = response.raw_data_sizes
            if not next_cursor:
                logger.debug("shard id = %s cursor = %s next cursor = %s count = %s size = %s",
                             shard_id, cursor, next_cursor,
                             response.get_log_count(),
                             log_raw_size
                             )
                if min_batch_fetch_bytes == -1:
                    return FetchTaskResult(fetch_log_group_list, cursor, log_raw_size)
                else:
                    return FetchTaskResult([fetch_log_group_list], cursor, log_raw_size)
            else:
                if min_batch_fetch_bytes == -1:
                    logger.debug("shard id = %s cursor = %s next cursor = %s count = %s size = %s",
                                 shard_id, cursor, next_cursor,
                                 response.get_log_count(), log_raw_size)
                    return FetchTaskResult(fetch_log_group_list, next_cursor, log_raw_size)
                else:
                    log_count = response.get_log_count()
                    log_group_list = [fetch_log_group_list]

                    ori_cursor = cursor
                    ret_fetched_result = FetchTaskResult([fetch_log_group_list], next_cursor, log_raw_size)
                    try:
                        while log_raw_size < min_batch_fetch_bytes:
                            cursor = next_cursor
                            response = loghub_client_adapter.pull_logs(shard_id, cursor,
                                                count=max_fetch_log_group_size, end_cursor=end_cursor)
                            next_cursor = response.get_next_cursor()
                            log_raw_size += response.raw_data_sizes
                            log_count += response.get_log_count()
                            log_group_list.append(response.get_loggroup_list())
                            ret_fetched_result = FetchTaskResult(log_group_list, next_cursor, log_raw_size)
                            if next_cursor == cursor:
                                break
                    except Exception as e:
                        # if has exception ,use origin fetch result
                        logger.warning('optimize fetch failed', extra={"event_id": "consumer_processor:consumer_fetch_task", "reason": str(e)},
                                       exc_info=True)
                    logger.debug("shard id = %s cursor = %s next cursor = %s count = %s size = %s",
                                 shard_id, ori_cursor, ret_fetched_result.cursor,
                                 log_count, ret_fetched_result.get_fetched_raw_data_sizes())
                    return ret_fetched_result
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
        logger.warning('Failed to call user shutodnw method', extra={"event_id": "consumer_processor:shutdown:call_shutdown", "reason": str(e)},
                     exc_info=True)
        exception = None

    try:
        check_point_tracker.get_check_point()
    except Exception as e:
        logger.error('Failed to flush check point', extra={"event_id": "consumer_processor:shutdown:fail_flush_checkpoint", "reason": str(e)}, exc_info=True)

    return TaskResult(exception)
