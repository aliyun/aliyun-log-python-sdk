# -×- coding: utf-8 -*-

import logging
import time

from threading import Thread

logger = logging.getLogger(__name__)


class ConsumerHeatBeat(Thread):

    def __init__(self, log_client, heartbeat_interval):
        super(ConsumerHeatBeat, self).__init__()
        self.log_client = log_client
        self.heartbeat_interval = heartbeat_interval
        self.mheld_shards = []
        self.mheart_shards = []
        self.shut_down_flag = False

    def run(self):
        logger.debug('heart beat start')
        while not self.shut_down_flag:
            try:
                response_shards = []
                self.log_client.heartbeat(self.mheart_shards, response_shards)
                self.mheld_shards = response_shards
                self.mheart_shards = self.mheld_shards[:]
                time.sleep(self.heartbeat_interval)
            except Exception as e:
                logger.warning("fail to heat beat", e)

    def get_held_shards(self):
        """
        must copy to prevent race condition in multi-threads
        :return:
        """
        return self.mheld_shards[:]

    def shutdown(self):
        logger.debug('heart beat stop')
        self.shut_down_flag = True

    def remove_heart_shard(self, shard):
        if shard in self.mheld_shards:
            self.mheart_shards.remove(shard)
