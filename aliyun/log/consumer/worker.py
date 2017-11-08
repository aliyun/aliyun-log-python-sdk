# -*- coding: utf-8 -*-


import logging
import time
from threading import Thread

from .consumer_client import ConsumerClient

from .heart_beat import ConsumerHeatBeat
from .shard_worker import ShardConsumerWorker


class ConsumerWorker(Thread):

    def __init__(self, make_processor, consumer_option):
        super(ConsumerWorker, self).__init__()
        self.make_processor = make_processor
        self.option = consumer_option
        self.consumer_client = \
            ConsumerClient(consumer_option.endpoint, consumer_option.accessKeyId, consumer_option.accessKey,
                           consumer_option.project, consumer_option.logstore, consumer_option.consumer_group_name,
                           consumer_option.consumer_name, consumer_option.securityToken)
        self.shut_down_flag = False
        self.logger = logging.getLogger(self.__class__.__name__)
        self.shard_consumers = {}

        self.consumer_client.create_consumer_group(consumer_option.heartbeat_interval*2, consumer_option.in_order)
        self.heart_beat = ConsumerHeatBeat(self.consumer_client, consumer_option.heartbeat_interval)

    def run(self):
        self.logger.debug('worker start')
        self.heart_beat.start()
        while not self.shut_down_flag:
            held_shards = self.heart_beat.get_held_shards()
            for shard in held_shards:
                shard_consumer = self._get_shard_consumer(shard)
                shard_consumer.consume()
            self.clean_shard_consumer(held_shards)
            try:
                time.sleep(self.option.data_fetch_interval)
            except Exception as e:
                print(e)

    def clean_shard_consumer(self, owned_shards):
        remove_shards = []
        # remove the shards that's not assigned by server
        for shard, consumer in self.shard_consumers.items():
            if shard not in owned_shards:
                consumer.shut_down()
                self.logger.info('Try to shut down unassigned consumer shard: ' + str(shard))
            if consumer.is_shutdown():
                self.heart_beat.remove_heart_shard(shard)
                remove_shards.append(shard)
                self.logger.info('Remove an unassigned consumer shard:' + str(shard))

        for shard in remove_shards:
            self.shard_consumers.pop(shard)

    def shutdown(self):
        self.shut_down_flag = True
        self.heart_beat.shutdown()
        self.logger.debug('worker stop')

    def _get_shard_consumer(self, shard_id):
        consumer = self.shard_consumers.get(shard_id, None)
        if consumer is not None:
            return consumer

        consumer = ShardConsumerWorker(self.consumer_client, shard_id, self.option.consumer_name,
                                       self.make_processor(),
                                       self.option.cursor_position, self.option.cursor_start_time)
        self.shard_consumers[shard_id] = consumer
        return consumer
