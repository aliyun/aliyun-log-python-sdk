# -*- coding: utf-8 -*-

import copy
import logging
import time

from .checkpoint_tracker import ConsumerCheckpointTracker

from .config import ConsumerStatus
from .fetched_log_group import FetchedLogGroup
from .tasks import ProcessTaskResult, InitTaskResult, FetchTaskResult
from .tasks import consumer_fetch_task, consumer_initialize_task, \
    consumer_process_task, consumer_shutdown_task
from .exceptions import ClientWorkerException


class ShardConsumerWorker(object):
    def __init__(self, log_client, shard_id, consumer_name, processor, cursor_position, cursor_start_time,
                 max_fetch_log_group_size=1000, executor=None):
        self.log_client = log_client
        self.shard_id = shard_id
        self.consumer_name = consumer_name
        self.cursor_position = cursor_position
        self.cursor_start_time = cursor_start_time
        self.processor = processor
        self.checkpoint_tracker = ConsumerCheckpointTracker(self.log_client, self.consumer_name,
                                                            self.shard_id)
        self.executor = executor
        self.max_fetch_log_group_size = max_fetch_log_group_size

        self.consumer_status = ConsumerStatus.INITIALIZING
        self.current_task_exist = False
        self.task_future = None
        self.fetch_data_future = None

        self.next_fetch_cursor = ''
        self.shutdown = False
        self.last_fetch_log_group = None

        self.last_log_error_time = 0
        self.last_fetch_time = 0
        self.last_fetch_count = 0

        self.logger = logging.getLogger(__name__)

    def consume(self):
        self.logger.debug('consumer start consuming')
        self.check_and_generate_next_task()
        if self.consumer_status == ConsumerStatus.PROCESSING and self.last_fetch_log_group is None:
            self.fetch_data()

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

    def fetch_data(self):
        # no task or it's done
        if self.fetch_data_future is None or self.fetch_data_future.done():
            task_result = self.get_task_result(self.fetch_data_future)

            # task is done, output results and get next_cursor
            if task_result is not None and task_result.get_exception() is None:
                assert isinstance(task_result, FetchTaskResult), \
                    ClientWorkerException("fetch result type is not as expected")

                self.last_fetch_log_group = FetchedLogGroup(self.shard_id, task_result.get_fetched_log_group_list(),
                                                            task_result.get_cursor())
                self.next_fetch_cursor = task_result.get_cursor()
                self.last_fetch_count = self.last_fetch_log_group.log_group_size

            self._sample_log_error(task_result)

            # no task or task is done, create new task
            if task_result is None or task_result.get_exception() is None:
                # flag to indicate if it's done
                is_generate_fetch_task = True

                # throttling control, similar as Java's SDK
                if self.last_fetch_count < 100:
                    is_generate_fetch_task = (time.time() - self.last_fetch_time) > 0.5
                elif self.last_fetch_count < 500:
                    is_generate_fetch_task = (time.time() - self.last_fetch_time) > 0.2
                elif self.last_fetch_count < 1000:
                    is_generate_fetch_task = (time.time() - self.last_fetch_time) > 0.05

                if is_generate_fetch_task:
                    self.last_fetch_time = time.time()
                    self.fetch_data_future = \
                        self.executor.submit(consumer_fetch_task,
                                             self.log_client, self.shard_id, self.next_fetch_cursor,
                                             max_fetch_log_group_size=self.max_fetch_log_group_size)
                else:
                    self.fetch_data_future = None
            else:
                self.fetch_data_future = None

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
                    assert self.consumer_status == ConsumerStatus.INITIALIZING, \
                        ClientWorkerException("get init task result, but status is: " + str(self.consumer_status))

                    init_result = task_result
                    self.next_fetch_cursor = init_result.get_cursor()
                    self.checkpoint_tracker.set_memory_check_point(self.next_fetch_cursor)
                    if init_result.is_cursor_persistent():
                        self.checkpoint_tracker.set_persistent_check_point(self.next_fetch_cursor)

                elif isinstance(task_result, ProcessTaskResult):
                    # maintain check points
                    process_task_result = task_result
                    roll_back_checkpoint = process_task_result.get_rollback_check_point()
                    if roll_back_checkpoint:
                        self.last_fetch_log_group = None
                        self.logger.info("user defined to roll-back check-point, cancel current fetching task")
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
        if self.consumer_status == ConsumerStatus.INITIALIZING:
            self.current_task_exist = True
            self.task_future = self.executor.submit(consumer_initialize_task, self.processor, self.log_client,
                                                    self.shard_id, self.cursor_position, self.cursor_start_time)

        elif self.consumer_status == ConsumerStatus.PROCESSING:
            if self.last_fetch_log_group is not None:
                self.checkpoint_tracker.set_cursor(self.last_fetch_log_group.end_cursor)
                self.current_task_exist = True

                # must deep copy cause some revision will happen
                last_fetch_log_group = copy.deepcopy(self.last_fetch_log_group)
                self.last_fetch_log_group = None

                if self.last_fetch_count > 0:
                    self.task_future = self.executor.submit(consumer_process_task, self.processor,
                                                            last_fetch_log_group.fetched_log_group_list,
                                                            self.checkpoint_tracker)

        elif self.consumer_status == ConsumerStatus.SHUTTING_DOWN:
            self.current_task_exist = True
            self.logger.info("start to cancel fetch job")
            self.cancel_current_fetch()
            self.task_future = self.executor.submit(consumer_shutdown_task, self.processor, self.checkpoint_tracker)

    def cancel_current_fetch(self):
        if self.fetch_data_future is not None:
            self.fetch_data_future.cancel()
            self.logger.info('Cancel a fetch task, shard id: ' + str(self.shard_id))
            self.fetch_data_future = None

    def _sample_log_error(self, result):
        # record the time when error happens
        current_time = time.time()
        if result is not None \
            and result.get_exception() is not None \
                and current_time - self.last_log_error_time > 5:
            self.logger.warning(result.get_exception(), exc_info=True)
            self.last_log_error_time = current_time

    def _update_status(self, task_succcess):
        if self.consumer_status == ConsumerStatus.SHUTTING_DOWN:
            # if no task or previous task suceeds, shutting-down -> shutdown complete
            if not self.current_task_exist or task_succcess:
                self.consumer_status = ConsumerStatus.SHUTDOWN_COMPLETE
        elif self.shutdown:
            # always set to shutting down (if flag is set)
            self.consumer_status = ConsumerStatus.SHUTTING_DOWN
        elif self.consumer_status == ConsumerStatus.INITIALIZING:
            # if initing and has task succeed, init -> processing
            if task_succcess:
                self.consumer_status = ConsumerStatus.PROCESSING

    def shut_down(self):
        """
        set shutting down flag, if not shutdown yet, complete the ongoing task(?)
        :return:
        """
        self.shutdown = True
        if not self.is_shutdown():
            self.check_and_generate_next_task()

    def is_shutdown(self):
        return self.consumer_status == ConsumerStatus.SHUTDOWN_COMPLETE
