# -*- coding: utf-8 -*-


import logging
import time
from threading import Thread

from .consumer_client import ConsumerClient

from .heart_beat import ConsumerHeatBeat
from .shard_worker import ShardConsumerWorker
from concurrent.futures import ThreadPoolExecutor


class ConsumerWorkerLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        consumer_worker = self.extra['consumer_worker']  # type: ConsumerWorker
        consumer_option = consumer_worker.option
        _id = '/'.join([
            consumer_option.project, consumer_option.logstore,
            consumer_option.consumer_group_name, consumer_option.consumer_name
        ])
        return "[{0}]{1}".format(_id, msg), kwargs


class ConsumerWorker(Thread):
    def __init__(self, make_processor, consumer_option, args=None,
                 kwargs=None):
        super(ConsumerWorker, self).__init__()
        self.make_processor = make_processor
        self.process_args = args or ()
        self.process_kwargs = kwargs or {}
        self.option = consumer_option
        self.consumer_client = \
            ConsumerClient(consumer_option.endpoint, consumer_option.accessKeyId, consumer_option.accessKey,
                           consumer_option.project, consumer_option.logstore, consumer_option.consumer_group_name,
                           consumer_option.consumer_name, consumer_option.securityToken,
                           credentials_refresher=consumer_option.credentials_refresher,
                           auth_version=consumer_option.auth_version, region=consumer_option.region)
        self.shut_down_flag = False
        self.logger = ConsumerWorkerLoggerAdapter(
            logging.getLogger(__name__), {"consumer_worker": self})
        self.shard_consumers = {}

        self.last_owned_consumer_finish_time = 0

        self.consumer_client.ensure_consumer_group_created(consumer_option.consumer_group_time_out, consumer_option.in_order)
        self.heart_beat = ConsumerHeatBeat(self.consumer_client, consumer_option.heartbeat_interval,
                                           consumer_option.consumer_group_time_out)

        if consumer_option.shared_executor is not None:
            self.own_executor = False
            self._executor = consumer_option.shared_executor
        else:
            self.own_executor = True
            self._executor = ThreadPoolExecutor(max_workers=consumer_option.worker_pool_size)

    @property
    def executor(self):
        return self._executor

    def _need_stop(self):
        """
        check if need to stop:
        1. end_cursor has been hit and there's no more shard assinged (wait for heatbeat_interval * 3)
        :return:
        """
        if not self.option.cursor_end_time:
            return False

        all_finish = True
        for shard, consumer in self.shard_consumers.items():
            if consumer.is_shutdown():
                continue

            # has not yet do any successful fetch yet or get some data
            if consumer.last_success_fetch_time == 0 or consumer.last_fetch_count > 0:
                return False

        # init self.last_owned_consumer_finish_time if it's None
        if all_finish and self.last_owned_consumer_finish_time == 0:
            self.last_owned_consumer_finish_time = time.time()

        if abs(time.time() - self.last_owned_consumer_finish_time) >= \
                self.option.consumer_group_time_out + self.option.heartbeat_interval:
            return True

        return False

    def run(self):
        self.logger.info('consumer worker "{0}" start '.format(self.option.consumer_name))
        self.heart_beat.start()

        while not self.shut_down_flag:
            held_shards = self.heart_beat.get_held_shards()

            last_fetch_time = time.time()
            for shard in held_shards:
                if self.shut_down_flag:
                    break

                shard_consumer = self._get_shard_consumer(shard)
                if shard_consumer is None:  # error when init consumer. shutdown directly
                    self.shutdown()
                    break

                shard_consumer.consume()

            self.clean_shard_consumer(held_shards)

            if self._need_stop():
                self.logger.info("all owned shards complete the tasks, owned shards: {0}".format(self.shard_consumers))
                self.shutdown()

            time_to_sleep = self.option.data_fetch_interval - (time.time() - last_fetch_time)
            while time_to_sleep > 0 and not self.shut_down_flag:
                time.sleep(min(time_to_sleep, 1))
                time_to_sleep = self.option.data_fetch_interval - (time.time() - last_fetch_time)

        # # stopping worker, need to cleanup all existing shard consumer
        self.logger.info('consumer worker "{0}" try to cleanup consumers'.format(self.option.consumer_name))
        self.shutdown_and_wait()

        if self.own_executor:
            self.logger.info('consumer worker "{0}" try to shutdown executors'.format(self.option.consumer_name))
            self._executor.shutdown()
            self.logger.info('consumer worker "{0}" stopped'.format(self.option.consumer_name))
        else:
            self.logger.info('executor is shared, consumer worker "{0}" stopped'.format(self.option.consumer_name))

    def start(self, join=False):
        """
        when calling with join=True, must call it in main thread, or else, the Keyboard Interrupt won't be caputured.
        :param join: default False, if hold on until the worker is stopped by Ctrl+C or other reasons.
        :return:
        """
        Thread.start(self)

        if join:
            try:
                while self.is_alive():
                    self.join(timeout=60)
                self.logger.info("worker {0} exit unexpected, try to shutdown it".format(self.option.consumer_name))
                self.shutdown()
            except KeyboardInterrupt:
                self.logger.info("*** try to exit **** ")
                self.shutdown()

    def shutdown_and_wait(self):
        while True:
            time.sleep(0.5)
            for shard, consumer in self.shard_consumers.items():
                if not consumer.is_shutdown():
                    consumer.shut_down()
                    break  # there's live consumer, no need to check, loop to next
            else:
                break   # all are shutdown, exit look

        self.shard_consumers.clear()

    def clean_shard_consumer(self, owned_shards):
        remove_shards = []
        # remove the shards that's not assigned by server
        for shard, consumer in self.shard_consumers.items():
            if shard not in owned_shards:
                self.logger.info('Try to call shut down for unassigned consumer shard: ' + str(shard))
                consumer.shut_down()
                self.logger.info('Complete call shut down for unassigned consumer shard: ' + str(shard))
            if consumer.is_shutdown():
                self.heart_beat.remove_heart_shard(shard)
                remove_shards.append(shard)
                self.logger.info('Remove an unassigned consumer shard:' + str(shard))

        for shard in remove_shards:
            self.shard_consumers.pop(shard)

    def shutdown(self):
        self.shut_down_flag = True
        self.heart_beat.shutdown()
        self.logger.info('get stop signal, start to stop consumer worker "{0}"'.format(self.option.consumer_name))

    def _get_shard_consumer(self, shard_id):
        consumer = self.shard_consumers.get(shard_id, None)
        if consumer is not None:
            return consumer

        try:
            processer = self.make_processor(*self.process_args, **self.process_kwargs)
        except Exception as ex:
            self.logger.error("fail to init processor {0} with parameters {1}, {2}, detail: {3}".format(
                self.make_processor, self.process_args, self.process_kwargs, ex, exc_info=True))
            return None

        consumer = ShardConsumerWorker(self.consumer_client, shard_id, self.option.consumer_name,
                                       processer,
                                       self.option.cursor_position, self.option.cursor_start_time,
                                       executor=self._executor,
                                       cursor_end_time=self.option.cursor_end_time,
                                       max_fetch_log_group_size=self.option.max_fetch_log_group_size,
                                       query=self.option.query)
        self.shard_consumers[shard_id] = consumer
        return consumer
