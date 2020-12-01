# -*- coding: utf-8 -*-


import logging
import time
from threading import Thread

from .consumer_client import ConsumerClient

from .fixed_resource_barrier import FixedResourceBarrier
from .unlimited_resource_barriier import UnlimitedResourceBarrier
from .heart_beat import ConsumerHeatBeat
from .shard_worker import ShardConsumerWorker
from concurrent.futures import ThreadPoolExecutor
import json


class ConsumerWorkerLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        consumer_worker = self.extra['consumer_worker']  # type: ConsumerWorker
        consumer_option = consumer_worker.option
        _id = '/'.join([
            consumer_option.project, consumer_option.logstore,
            consumer_option.consumer_group_name, consumer_option.consumer_name
        ])
        kwargs['extra'] = kwargs.get('extra', {})
        kwargs['extra'].update({
            "etl_context": """{
            "project": "%s", 
            "logstore": "%s", 
            "consumer_group": "%s", 
            "consumer": "%s"}""" % (consumer_option.project,
                                         consumer_option.logstore,
                                         consumer_option.consumer_group_name,
                                         consumer_option.consumer_name)
        })

        return "[{0}]{1}".format(_id, msg), kwargs


class ConsumerWorker(Thread):
    def __init__(self, make_processor, consumer_option, args=None,
                 kwargs=None):
        super(ConsumerWorker, self).__init__()
        self.option = consumer_option
        self.logger = ConsumerWorkerLoggerAdapter(
            logging.getLogger(__name__), {"consumer_worker": self})
        self._exception = None

        extra = {
            "event_id": "consumer_worker:init:start",
            "extra_info_params": json.dumps({
                "processor": str(make_processor),
                "args": str(args),
                "kwargs": str(kwargs),
                "endpoint": consumer_option.endpoint,
                "ak_id": consumer_option.accessKeyId and consumer_option.accessKeyId[
                                                                :2] + "****" + consumer_option.accessKeyId[-2:],
                "timeout": consumer_option.consumer_group_time_out,
                "heatbeat_interval": consumer_option.heartbeat_interval,
                "fetch_interval": consumer_option.data_fetch_interval,
                "in_order": consumer_option.in_order,
                "cursor_pos": str(consumer_option.cursor_position),
                "start_time": consumer_option.cursor_start_time,
                "end_time": consumer_option.cursor_end_time,
                "max_fetch_size": consumer_option.max_fetch_log_group_size,
                "share_executor": consumer_option.shared_executor is not None,
                "pool_size": consumer_option.worker_pool_size,
                "max_in_progressing_data_size_in_mB" : consumer_option.max_in_progressing_data_size_in_mB
            })
        }
        self.max_fetch_log_group_size = consumer_option.max_fetch_log_group_size

        if consumer_option.max_in_progressing_data_size_in_mB > 0:
            self.resource_barrier = FixedResourceBarrier(
                                    consumer_option.max_in_progressing_data_size_in_mB * 1024 * 1024)
        else:
            self.resource_barrier = UnlimitedResourceBarrier()

        self.shard_count = 0
        self.min_batch_fetch_bytes = consumer_option.min_batch_fetch_bytes
        self.logger.debug(
            "start to init consumer worker, %s", extra, extra=extra)

        try:
            self.make_processor = make_processor
            self.process_args = args or ()
            self.process_kwargs = kwargs or {}
            self.consumer_client = \
                ConsumerClient(consumer_option.endpoint, consumer_option.accessKeyId, consumer_option.accessKey,
                               consumer_option.project, consumer_option.logstore, consumer_option.consumer_group_name,
                               consumer_option.consumer_name, consumer_option.securityToken,
                               consumer_option.metric_fields_from_scheduler,consumer_option.flush_check_metric_interval,
                               credentials_refresher=consumer_option.credentials_refresher,
                               )
            self._shut_down_flag = False
            self.shard_consumers = {}

            self.last_owned_consumer_finish_time = 0

            self.consumer_client.create_consumer_group(consumer_option.consumer_group_time_out,
                                                       consumer_option.in_order)
            self.heart_beat = ConsumerHeatBeat(self.consumer_client, consumer_option.heartbeat_interval,
                                               consumer_option.consumer_group_time_out)

            if consumer_option.shared_executor is not None:
                self.own_executor = False
                self._executor = consumer_option.shared_executor
            else:
                self.own_executor = True
                self._executor = ThreadPoolExecutor(max_workers=consumer_option.worker_pool_size)

            self._timeout_before_finish_for_batch = (self.option.consumer_group_time_out + self.option.heartbeat_interval) * 2

            extra['event_id'] = 'consumer_worker:init:complete'
            self.logger.debug(
                "complete to init consumer worker, %s", extra, extra=extra)
        except Exception as ex:
            extra['event_id'] = 'consumer_worker:init:fail'
            extra['reason'] = ex
            self.logger.error(
                "fail to init consumer worker, %s", extra, extra=extra, exc_info=True)
            raise ex

    @property
    def executor(self):
        return self._executor

    @property
    def timeout_before_finish_for_batch(self):
        return self._timeout_before_finish_for_batch

    @timeout_before_finish_for_batch.setter
    def timeout_before_finish_for_batch(self, value):
        self._timeout_before_finish_for_batch = value

    def _need_stop_for_batch_mode(self):
        """
        check if need to stop:
        1. end_cursor has been hit and there's no more shard assinged (wait for heatbeat_interval * 3)
        :return:
        """
        if not self.option.cursor_end_time:
            return False

        for shard, consumer in self.shard_consumers.items():
            if consumer.is_shutdown():
                continue

            # has not yet do any successful fetch yet or get some data
            if consumer.last_success_fetch_time == 0 or consumer.last_fetch_count > 0:
                self.last_owned_consumer_finish_time = 0
                return False

        # init self.last_owned_consumer_finish_time if it's None
        if self.last_owned_consumer_finish_time == 0:
            self.last_owned_consumer_finish_time = time.time()

        time_passed = abs(time.time() - self.last_owned_consumer_finish_time)
        time_out = self._timeout_before_finish_for_batch

        # if no consumer owned (probably initialized stage) to overcome the case server side's assignment is delayed.
        if not self.shard_consumers:
            time_out += min(self.option.heartbeat_interval, 5)
        if time_passed > time_out:
            return True

        return False

    def run(self):
        """
        main thread to consume data
        :return:
        """

        self.logger.debug('consumer worker start to run', extra={"event_id": "consumer_worker:run:enter"})
        self.heart_beat.start()
        last_fetch_throttle_min_shard = 0
        while not self._shut_down_flag:
            held_shards = self.heart_beat.get_held_shards()

            last_fetch_time = time.time()
            cur_fetch_throttle_min_shard = -1
            for shard in held_shards:
                if self._shut_down_flag:
                    self.logger.debug("shard consumer stop consumption")
                    break

                shard_consumer = self._get_shard_consumer(shard)
                if shard_consumer is None:  # error when init consumer. shutdown directly
                    self.logger.critical(
                        "fail to generate shard consumer for assigned shard %s, consumer will start to shutdown.",
                        shard, extra={"event_id": "consumer_worker:run:fail",
                                      "reason": "generate shard consumer for assigned shard %s" % shard})
                    self.shutdown()
                    break
                if not shard_consumer.consume(shard >= last_fetch_throttle_min_shard):
                    if cur_fetch_throttle_min_shard < 0:
                        cur_fetch_throttle_min_shard = shard
            last_fetch_throttle_min_shard = max(cur_fetch_throttle_min_shard, 0)

            self._check_shard_consumer_status(held_shards)

            if self._need_stop_for_batch_mode():
                self.logger.info("batch mode, all owned shards ({0}) complete the tasks, will stop the consumer".format(
                    list(self.shard_consumers.keys())), extra={"event_id": "consumer_worker:exit:start", "extra_info_params": """{"shards": "%s", }""" % list(self.shard_consumers.keys()), "reason": "batch_mode_complete" })
                self.shutdown()
                continue

            time_to_sleep = self.option.data_fetch_interval - (time.time() - last_fetch_time)
            while time_to_sleep > 0 and not self._shut_down_flag:
                time.sleep(min(time_to_sleep, 1))
                time_to_sleep = self.option.data_fetch_interval - (time.time() - last_fetch_time)

        # # stopping worker, need to cleanup all existing shard consumer
        self._shutdown_consumer_and_wait()

        if self.own_executor:
            self.logger.debug('consumer worker "{0}" try to shutdown executors'.format(self.option.consumer_name), extra={"event_id": "consumer_worker:run:try_to_shutdown_executor"})
            self._executor.shutdown()
            self.logger.debug('consumer worker "{0}" stopped'.format(self.option.consumer_name), extra={"event_id": "consumer_worker:run:coplete_shutdown"})
        else:
            self.logger.debug('executor is shared, consumer worker "{0}" stopped'.format(self.option.consumer_name), extra={"event_id": "consumer_worker:run:coplete_shutdown"})

    def start(self, join=False):
        """
        when calling with join=True, must call it in main thread, or else, the Keyboard Interrupt won't be caputured.
        :param join: default False, if hold on until the worker is stopped by Ctrl+C or other reasons.
        :return:
        """
        Thread.start(self)
        self.logger.debug("start the consumer worker thread, join: %s", join, extra={"event_id": "consumer_worker:init:complete", "extra_info_params": """{"join": "%s" }""" % join})

        if join:
            self.logger.debug("start to monitor the consumer worker thread", extra={"event_id": "consumer_worker:start:enter_join"})
            try:
                while self.is_alive():
                    self.join(timeout=60)
                if not self._shut_down_flag:
                    self.logger.error("consumer worker exit unexpectedly, try to shutdown it", extra={"event_id": "consumer_worker:start:thread_exit_unknown", "reason": "unkown exception happens"})
                    self.shutdown()
            except KeyboardInterrupt:
                self.logger.info("get stop signal (Ctrl+C), start to exit", extra={"event_id": "consumer_worker:exit:start", "reason": "ctrl_c_signal"})
                self.shutdown()

            while self.is_alive():
                self.join(timeout=60)
        if self._exception is not None:
            raise self._exception
        if self.heart_beat.exception is not None:
            raise self.heart_beat.exception

    def get_shard_count(self):
        return self.shard_count

    def _shutdown_consumer_and_wait(self):
        self.logger.debug('consumer worker start to stop all shard consumers (shards: %s)',
                         list(self.shard_consumers.keys()), extra={"event_id": "consumer_worker:exit:start_stop_all_shard_consumer", "extra_info_params": """{"shards": "%s"}""" % list(self.shard_consumers.keys())})
        while True:
            time.sleep(0.5)
            for shard, consumer in self.shard_consumers.items():
                if not consumer.is_shutdown():
                    consumer.shut_down()
                    break  # there's live consumer, no need to check, loop to next
            else:
                self.logger.info("all shard consumer worker stopped. (shards: %s)", list(self.shard_consumers.keys()), extra={"event_id": "consumer_worker:exit:all_shard_consumer", "extra_info_params": """{"shards": "%s" }""" % list(self.shard_consumers.keys())})
                break  # all are shutdown, exit look

        for shard, consumer in self.shard_consumers.items():
            consumer.checkpoint_tracker.stop_refresher()
        self.shard_consumers.clear()

    def _check_shard_consumer_status(self, owned_shards):
        remove_shards = []
        # remove the shards that's not assigned by server
        for shard, consumer in self.shard_consumers.items():
            if consumer.is_shutdown():
                self.logger.info('shard %s is stopped, remove it from heat beat list.', shard, extra={"event_id": "consumer_worker:heatbeat:unload_shard_consumer", "extra_info_params": """{"shard": "%s"}""" % shard})
                self.heart_beat.remove_heart_shard(shard)
                remove_shards.append(shard)
                consumer.checkpoint_tracker.stop_refresher()
                continue

            if shard not in owned_shards:
                self.logger.debug('shard %s is unassigned, notify shard worker to stop.', shard, extra={"event_id": "consumer_worker:run:shutdown_unassigned_shard_consumer", "extra_info_params": """{"shard": "%s"}""" % shard})
                consumer.shut_down()

        for shard in remove_shards:
            self.shard_consumers.pop(shard)

    def shutdown(self):
        self.logger.debug('Start to stop consumer worker, notify shard consumer worker and heatbeat to stop.', extra={"event_id": "consumer_worker:shutdown:enter"})
        self._shut_down_flag = True
        self.heart_beat.shutdown()

    def _get_shard_consumer(self, shard_id):
        consumer = self.shard_consumers.get(shard_id, None)
        if consumer is not None:
            return consumer

        self.logger.debug("shard %s is newly assigned, start to init shard consumer worker", shard_id, extra={"event_id": "consumer_worker:heatbeat:start_init_shard_consumer", "extra_info_params": """{"shard": "%s" }""" % shard_id})

        try:
            processor = self.make_processor(*self.process_args, **self.process_kwargs)
        except Exception as ex:
            self._exception = ex
            msg = "fail to call shard {0} processor {1} with parameters {2}, {3}, detail: {4}".format(
                shard_id, self.make_processor, self.process_args, self.process_kwargs, ex)
            self.logger.error(msg, extra={"event_id": "consumer_worker:heatbeat:fail_init_shard_consumer", "reason": msg, "extra_info_params": """{"shard": "%s" }""" % shard_id}, exc_info=True)
            return None

        try:
            consumer = ShardConsumerWorker(self.consumer_client, shard_id, self.option.consumer_name,
                                           processor,
                                           self.option.cursor_position, self.option.cursor_start_time,
                                           executor=self._executor,
                                           cursor_end_time=self.option.cursor_end_time,
                                           max_fetch_log_group_size=self.max_fetch_log_group_size,
                                           resource_barrier=self.resource_barrier,
                                           min_batch_fetch_bytes=self.min_batch_fetch_bytes)
        except Exception as ex:
            self._exception = ex
            msg = "fail to init shard consumer worker for shard %s, error: %s" % (shard_id, ex)
            self.logger.error(msg, extra={"event_id": "consumer_worker:heatbeat:fail_init_shard_consumer", "reason": msg, "extra_info_params": """{"shard": "%s" }""" % shard_id}, exc_info=True)
            return None

        self.shard_consumers[shard_id] = consumer
        self.logger.info("complete to init shard consumer worker for shard: %s", shard_id, extra={"event_id": "consumer_worker:heatbeat:init_shard_consumer", "extra_info_params": """{"shard": "%s"}""" % shard_id})
        return consumer
