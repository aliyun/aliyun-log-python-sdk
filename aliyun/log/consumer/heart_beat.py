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
        logger.info('heart beat start')
        last_heatbeat_time = 0
        while not self.shut_down_flag:
            try:
                response_shards = []
                self.log_client.heartbeat(self.mheart_shards, response_shards)
                logger.info('heart beat result: {} get: {}'.format(self.mheart_shards, response_shards))
                self.mheld_shards = response_shards
                self.mheart_shards = self.mheld_shards[:]

                # default sleep for 2s from "LogHubConfig"
                time_to_sleep = self.heartbeat_interval - (time.time() - last_heatbeat_time)
                last_heatbeat_time = time.time()
                if time_to_sleep > 0 and not self.shut_down_flag:
                    time.sleep(time_to_sleep)
            except Exception as e:
                logger.warning("fail to heat beat", e)

        logger.info('heart beat exit')

    def get_held_shards(self):
        """
        must copy to prevent race condition in multi-threads
        :return:
        """
        return self.mheld_shards[:]

    def shutdown(self):
        logger.info('try to stop heart beat')
        self.shut_down_flag = True

    def remove_heart_shard(self, shard):
        logger.info('try to remove shard "{0}", current shard: {1}'.format(shard, self.mheld_shards))
        if shard in self.mheld_shards:
            self.mheld_shards.remove(shard)
        if shard in self.mheart_shards:
            self.mheart_shards.remove(shard)
