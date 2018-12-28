# -*- coding: utf-8 -*-


import logging
import time
from threading import Thread

from .consumer_client import ConsumerClient

from .heart_beat import ConsumerHeatBeat
from .shard_worker import ShardConsumerWorker
from concurrent.futures import ThreadPoolExecutor


class ConsumerWorker(Thread):

    def __init__(self, make_processor, consumer_option, args=None, kwargs=None):
        super(ConsumerWorker, self).__init__()
        self.make_processor = make_processor
        self.process_args = args or ()
        self.process_kwargs = kwargs or {}
        self.option = consumer_option
        self.consumer_client = \
            ConsumerClient(consumer_option.endpoint, consumer_option.accessKeyId, consumer_option.accessKey,
                           consumer_option.project, consumer_option.logstore, consumer_option.consumer_group_name,
                           consumer_option.consumer_name, consumer_option.securityToken)
        self.shut_down_flag = False
        self.logger = logging.getLogger(__name__)
        self.shard_consumers = {}

        self.consumer_client.create_consumer_group(consumer_option.heartbeat_interval*2, consumer_option.in_order)
        self.heart_beat = ConsumerHeatBeat(self.consumer_client, consumer_option.heartbeat_interval)

        self.executor = ThreadPoolExecutor(max_workers=consumer_option.worker_pool_size)

    def run(self):
        self.logger.info('consumer worker "{0}" start '.format(self.option.consumer_name))
        self.heart_beat.start()

        last_fetch_time = 0
        while not self.shut_down_flag:
            held_shards = self.heart_beat.get_held_shards()

            for shard in held_shards:
                if self.shut_down_flag:
                    break

                shard_consumer = self._get_shard_consumer(shard)
                shard_consumer.consume()

            self.clean_shard_consumer(held_shards)

            # default sleep for 2s from "LogHubConfig"
            time_to_sleep = self.option.data_fetch_interval - (time.time() - last_fetch_time)
            last_fetch_time = time.time()
            if time_to_sleep > 0 and not self.shut_down_flag:
                time.sleep(time_to_sleep)

        # # stopping worker, need to cleanup all existing shard consumer
        self.logger.info('consumer worker "{0}" try to cleanup consumers'.format(self.option.consumer_name))
        self.shutdown_and_wait()

        self.logger.info('consumer worker "{0}" try to shutdown executors'.format(self.option.consumer_name))
        self.executor.shutdown()

        self.logger.info('consumer worker "{0}" stopped'.format(self.option.consumer_name))

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

        consumer = ShardConsumerWorker(self.consumer_client, shard_id, self.option.consumer_name,
                                       self.make_processor(*self.process_args, **self.process_kwargs),
                                       self.option.cursor_position, self.option.cursor_start_time,
                                       executor=self.executor)
        self.shard_consumers[shard_id] = consumer
        return consumer

