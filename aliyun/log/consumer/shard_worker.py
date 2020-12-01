# -*- coding: utf-8 -*-

import logging
import time

from .checkpoint_tracker import ConsumerCheckpointTracker

from .config import ConsumerStatus
from .config import PRE_ALLOCATED_BYTES
from .fetched_log_group import FetchedLogGroup
from .tasks import ProcessTaskResult, InitTaskResult, FetchTaskResult, TaskResult
from .tasks import consumer_fetch_task, consumer_initialize_task, \
    consumer_process_task, consumer_shutdown_task
from .exceptions import ClientWorkerException


class ShardConsumerWorkerLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        shard_consumer_worker = self.extra[
            'shard_consumer_worker']  # type: ShardConsumerWorker
        consumer_client = shard_consumer_worker.log_client
        _id = '/'.join([
            consumer_client.mproject, consumer_client.mlogstore,
            consumer_client.mconsumer_group, consumer_client.mconsumer,
            str(shard_consumer_worker.shard_id)
        ])

        kwargs['extra'] = kwargs.get('extra', {})
        kwargs['extra'].update({
            "etl_context": """{
            "project": "%s", 
            "logstore": "%s", 
            "consumer_group": "%s", 
            "consumer": "%s",
            "shard_id": "%s"} """ % (consumer_client.mproject,
                                          consumer_client.mlogstore,
                                          consumer_client.mconsumer_group,
                                          consumer_client.mconsumer,
                                          shard_consumer_worker.shard_id)
        })

        return "[{0}]{1}".format(_id, msg), kwargs


class ShardConsumerWorker(object):
    """
    no threading worker
    """
    def __init__(self, log_client, shard_id, consumer_name, processor, cursor_position, cursor_start_time,
                 max_fetch_log_group_size=1000, executor=None, cursor_end_time=None, resource_barrier=None,
                 min_batch_fetch_bytes=-1):
        self.log_client = log_client
        self.shard_id = shard_id

        # note the dependency: log_client and shard_id of logger
        self.logger = ShardConsumerWorkerLoggerAdapter(
            logging.getLogger(__name__), {"shard_consumer_worker": self})

        self.logger.debug("start to init shard consumer worker, max_fetch_log_group_size: %s" % max_fetch_log_group_size,
                          extra={"event_id": "shard_consumer_worker:init:enter"})

        self.consumer_name = consumer_name
        self.cursor_position = cursor_position
        self.cursor_start_time = cursor_start_time
        self.cursor_end_time = cursor_end_time or None
        self.processor = processor
        self.checkpoint_tracker = ConsumerCheckpointTracker(
            self.log_client,
            self.consumer_name,
            self.shard_id,
            cursor_end_time=self.cursor_end_time,
            shard_worker=self
        )
        self.executor = executor
        self.max_fetch_log_group_size = max_fetch_log_group_size

        self._consumer_status = ConsumerStatus.INITIALIZING
        self.current_task_exist = False
        self.task_future = None
        self.fetch_data_future = None

        self.next_fetch_cursor = ''
        self.fetch_end_cursor = None

        self._shutdown_flag = False
        self.last_fetch_log_group = None

        self.last_log_error_time = 0
        self.last_fetch_time = 0
        self.last_fetch_count = 0
        self.last_success_fetch_time = 0

        self.resource_barrier = resource_barrier
        self.throttled_count = 0
        self.last_un_throttled_time = int(time.time())
        self.min_batch_fetch_bytes = min_batch_fetch_bytes

        self.logger.debug("complete to init shard consumer worker", extra={"event_id": "shard_consumer_worker:init:complete"})

    def consume(self, fetch_allowed=True):
        self.logger.debug('consumer start consuming, fetch_allowed: %s' % fetch_allowed)
        self.check_and_generate_next_task()
        if self._consumer_status == ConsumerStatus.PROCESSING and self.last_fetch_log_group is None:
            if not self.fetch_data(fetch_allowed):
                # if shard fetch data is throttled if fetch_allowed=True, return false
                # else if shard fetch data is not allow, return true
                return not fetch_allowed
        return True

    @staticmethod
    # get future (if failed return None)
    def get_task_result(task_future):
        if task_future is not None and task_future.done():
            try:
                return task_future.result()
            except Exception:
                import traceback
                traceback.print_exc()
        return None

    def check_throttled(self):
        if self.resource_barrier.tryAcquire(self.shard_id, PRE_ALLOCATED_BYTES):
            self.last_un_throttled_time = int(time.time())
            return False
        self.throttled_count += 1
        if self.throttled_count % 200 == 0:
            self.logger.info("fetch request throttled , shard %s" % self.shard_id)
            self.throttled_count = 0

            if int(time.time()) > self.last_un_throttled_time + 900:
                msg = "shard : %s is throttled more than 15min" % self.shard_id
                extra = {"event_id": "shard_worker:check_throttled:warning", "reason": msg}
                extra["error_code"] = "ShardConsumeThrottledWarning"
                self.logger.warn(msg, extra=extra)
        return True

    def should_fetch_next(self, has_error):
        if has_error:
            return False
        # throttling control, similar as Java's SDK
        if self.last_fetch_count < 100:
            return ((time.time() - self.last_fetch_time) > 0.5) and (not self.check_throttled())
        elif self.last_fetch_count < 500:
            return ((time.time() - self.last_fetch_time) > 0.2) and (not self.check_throttled())
        elif self.last_fetch_count < 1000:
            return ((time.time() - self.last_fetch_time) > 0.05) and (not self.check_throttled())

        return not self.check_throttled()

    def fetch_data(self, fetch_allowed):
        # no task or it's done
        has_error = False
        if self.fetch_data_future is not None:
            if self.fetch_data_future.cancelled():
                self.fetch_data_future = None
                self.last_fetch_log_group = None
                self.resource_barrier.release(self.shard_id, PRE_ALLOCATED_BYTES)
                self.logger.debug("shard: %s fetch_data_future is cancelled" % self.shard_id)
                return True
            elif not self.fetch_data_future.done():
                return True
            task_result = self.get_task_result(self.fetch_data_future)
            if task_result is not None and task_result.get_exception() is None:
                assert isinstance(task_result, FetchTaskResult), \
                    ClientWorkerException("fetch result type is not as expected")

                self.last_success_fetch_time = time.time()

                self.last_fetch_raw_size = task_result.get_fetched_raw_data_sizes()
                self.last_fetch_log_group = FetchedLogGroup(self.shard_id, task_result.get_fetched_log_group_list(),
                                                            task_result.get_cursor(),
                                                            self.last_fetch_raw_size)
                self.next_fetch_cursor = task_result.get_cursor()
                self.last_fetch_count = self.last_fetch_log_group.log_group_count

                self.resource_barrier.acquire(self.shard_id, self.last_fetch_raw_size - PRE_ALLOCATED_BYTES)
            else:
                self.resource_barrier.release(self.shard_id, PRE_ALLOCATED_BYTES)
            has_error = task_result is not None and task_result.get_exception() is not None
            self._sample_log_error(task_result)

        if fetch_allowed and self.should_fetch_next(has_error):
            self.last_fetch_time = time.time()
            self.logger.debug("shard: %s, pull_log_group size: %s" % (self.shard_id, self.max_fetch_log_group_size))
            self.fetch_data_future = \
                self.executor.submit(consumer_fetch_task,
                                     self.log_client, self.shard_id, self.next_fetch_cursor,
                                     max_fetch_log_group_size=self.max_fetch_log_group_size,
                                     end_cursor=self.fetch_end_cursor, min_batch_fetch_bytes=self.min_batch_fetch_bytes)
            return True
        else:
            self.fetch_data_future = None
            return False

    def check_and_generate_next_task(self):
        """
        check if the previous task is done and proceed to fire another task
        :return:
        """
        # if self.task_future is None:
        #     # there's no any ongoing task
        #     self._update_status(False)
        #     self.generate_next_task()
        #     return

        if self.task_future is None or self.task_future.done():
            task_success = False


            task_result = self.get_task_result(self.task_future)
            self.task_future = None
            if task_result is not None and task_result.get_exception() is None:

                task_success = True
                if isinstance(task_result, InitTaskResult):
                    # maintain check points
                    assert self._consumer_status == ConsumerStatus.INITIALIZING, \
                        ClientWorkerException("get init task result, but status is: " + str(self._consumer_status))

                    init_result = task_result
                    self.next_fetch_cursor = init_result.get_cursor()
                    self.fetch_end_cursor = init_result.end_cursor
                    self.checkpoint_tracker.set_memory_check_point(self.next_fetch_cursor)
                    if init_result.is_cursor_persistent():
                        self.checkpoint_tracker.set_persistent_check_point(self.next_fetch_cursor)

                elif isinstance(task_result, ProcessTaskResult):
                    # maintain check points
                    process_task_result = task_result
                    roll_back_checkpoint = process_task_result.get_rollback_check_point()
                    if roll_back_checkpoint:
                        self.last_fetch_log_group = None
                        self.logger.info("user defined to roll-back check-point, cancel current fetching task", extra={"event_id": "shard_consumer_worker:run:rollback_checkpoint", "extra_info_params": """{"rollback_checkpoint": "%s"}""" % roll_back_checkpoint})
                        self.cancel_current_fetch()
                        self.next_fetch_cursor = roll_back_checkpoint

            # log task status
            self._sample_log_error(task_result)

            # update status basing on task results
            self._update_status(task_success)

            #
            self._generate_next_task()

    def _generate_next_task(self):
        """
        submit consumer framework defined task
        :return:
        """
        if self._consumer_status == ConsumerStatus.INITIALIZING:
            self.logger.debug("shard: %s consumer status is initalizing" % self.shard_id)
            self.current_task_exist = True
            self.task_future = self.executor.submit(consumer_initialize_task, self.processor, self.log_client,
                                                    self.shard_id, self.cursor_position, self.cursor_start_time, self.cursor_end_time)

        elif self._consumer_status == ConsumerStatus.PROCESSING:
            self.logger.debug("shard: %s consumer status is processing" % self.shard_id)
            if self.last_fetch_log_group is not None:
                self.logger.debug("shard: %s last_fetch_log_group is not None" % self.shard_id)
                self.checkpoint_tracker.set_cursor(self.last_fetch_log_group.end_cursor)
                self.current_task_exist = True

                # use the same object here, because it won't be changed later
                last_fetch_log_group = self.last_fetch_log_group
                self.last_fetch_log_group = None

                if self.last_fetch_count > 0:
                    self.logger.debug("shard: %s start to submit process_task, bytes: %s" % (self.shard_id,
                                      last_fetch_log_group.fetched_log_raw_data_sizes))
                    self.task_future = self.executor.submit(consumer_process_task, self.processor,
                                                            last_fetch_log_group.fetched_log_group_list,
                                                            self.checkpoint_tracker,
                                                            last_fetch_log_group.fetched_log_raw_data_sizes,
                                                            self.shard_id,
                                                            self.resource_barrier)
            else:
                self.logger.debug("shard: %s last_fetch_log_group is None" % self.shard_id)

        elif self._consumer_status == ConsumerStatus.SHUTTING_DOWN:
            if self.last_fetch_log_group is not None:
                self.resource_barrier.release(self.shard_id, self.last_fetch_log_group.fetched_log_raw_data_sizes)
            self.logger.debug("shard: %s consumer status is shutting_now" % self.shard_id)
            self.current_task_exist = True
            self.logger.debug("shard: %s start to cancel fetch job" % self.shard_id,
                              extra={"event_id": "shard_consumer_worker:exit:start_cancel_job"})
            self.cancel_current_fetch()
            self.task_future = self.executor.submit(consumer_shutdown_task, self.processor, self.checkpoint_tracker)
        else:
            self.logger.debug("shard: %s consumer status is %s" % (self.shard_id, self._consumer_status))

    def is_processing_data(self):
        if self.last_fetch_count > 0 or self.task_future is not None:
            return True
        else:
            return False

    def cancel_current_fetch(self):
        if self.fetch_data_future is not None:
            self.fetch_data_future.cancel()
            self.last_fetch_log_group = None
            self.resource_barrier.release(self.shard_id, PRE_ALLOCATED_BYTES)

            self.logger.debug('shard: %s Cancel a fetch task,' % self.shard_id,
                              extra={"event_id": "shard_consumer_worker:exit:cancel_job"})
            self.fetch_data_future = None

    def _sample_log_error(self, result):
        # record the time when error happens
        if not isinstance(result, TaskResult):
            return

        exc = result.get_exception()
        if exc is None:
            return

        current_time = time.time()
        if current_time - self.last_log_error_time <= 5:
            return

        self.logger.warning(exc, extra={"event_id": "shard_consumer:process:sampe_error", "reason": str(exc)}, exc_info=result.exc_info)
        self.last_log_error_time = current_time

    def _update_status(self, task_succcess):
        if self._consumer_status == ConsumerStatus.SHUTTING_DOWN:
            # if no task or previous task suceeds, shutting-down -> shutdown complete
            if not self.current_task_exist or task_succcess:
                self._consumer_status = ConsumerStatus.SHUTDOWN_COMPLETE
        elif self._shutdown_flag:
            # always set to shutting down (if flag is set)
            self._consumer_status = ConsumerStatus.SHUTTING_DOWN
        elif self._consumer_status == ConsumerStatus.INITIALIZING:
            # if initing and has task succeed, init -> processing
            if task_succcess:
                self._consumer_status = ConsumerStatus.PROCESSING

    def shut_down(self):
        """
        set shutting down flag, if not shutdown yet, complete the ongoing task(?)
        :return:
        """
        self.logger.debug("shard: %s get shutdown command" % self.shard_id)
        self._shutdown_flag = True
        if not self.is_shutdown():
            self.check_and_generate_next_task()

    def is_shutdown(self):
        return self._consumer_status == ConsumerStatus.SHUTDOWN_COMPLETE
