# -×- coding: utf-8 -*-

import logging
import time

from threading import Thread
from multiprocessing import RLock

class HeartBeatLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        heart_beat = self.extra['heart_beat']  # type: ConsumerHeatBeat
        _id = '/'.join([
            heart_beat.log_client.mproject, heart_beat.log_client.mlogstore,
            heart_beat.log_client.mconsumer_group,
            heart_beat.log_client.mconsumer
        ])
        return "[{0}]{1}".format(_id, msg), kwargs


class ConsumerHeatBeat(Thread):

    def __init__(self, log_client, heartbeat_interval, consumer_group_time_out):
        super(ConsumerHeatBeat, self).__init__()
        self.log_client = log_client
        self.heartbeat_interval = heartbeat_interval
        self.mheld_shards = []
        self.mheart_shards = []
        self.shut_down_flag = False
        self.lock = RLock()
        self.last_hearbeat_successed_unixtime = time.time()
        self.consumer_group_time_out = consumer_group_time_out
        self.logger = HeartBeatLoggerAdapter(
            logging.getLogger(__name__), {"heart_beat": self})

    def run(self):
        self.logger.info('heart beat start')
        while not self.shut_down_flag:
            try:
                response_shards = []
                last_heatbeat_time = time.time()

                if self.log_client.heartbeat(self.mheart_shards, response_shards):
                    self.last_hearbeat_successed_unixtime = time.time()
                    self.logger.debug('heart beat result: %s get: %s',
                                      self.mheart_shards, response_shards)
                    if self.mheart_shards != response_shards:
                        current_set, response_set = set(
                            self.mheart_shards), set(response_shards)
                        add_set = response_set - current_set
                        remove_set = current_set - response_set
                        if any([add_set, remove_set]):
                            self.logger.info(
                                "shard reorganize, adding: %s, removing: %s",
                                add_set, remove_set)
                else:
                    if time.time() - self.last_hearbeat_successed_unixtime > \
                            (self.consumer_group_time_out + self.heartbeat_interval):
                        response_shards = []
                        self.logger.info(
                            "Heart beat timeout, automatic reset consumer held shards")
                    else:
                        with self.lock:
                            response_shards = self.mheld_shards
                            self.logger.info(
                                "Heart beat failed, Keep the held shards unchanged")

                with self.lock:
                    self.mheart_shards = list(set(self.mheart_shards + response_shards))
                    self.mheld_shards = response_shards

                # default sleep for 2s from "LogHubConfig"
                time_to_sleep = self.heartbeat_interval - (time.time() - last_heatbeat_time)
                while time_to_sleep > 0 and not self.shut_down_flag:
                    time.sleep(min(time_to_sleep, 1))
                    time_to_sleep = self.heartbeat_interval - (time.time() - last_heatbeat_time)
            except Exception as e:
                self.logger.warning("fail to heat beat", e)

        self.logger.info('heart beat exit')

    def get_held_shards(self):
        """
        must copy to prevent race condition in multi-threads
        :return:
        """
        return self.mheld_shards[:]

    def shutdown(self):
        self.logger.info('try to stop heart beat')
        self.shut_down_flag = True

    def remove_heart_shard(self, shard):
        self.logger.info('try to remove shard "{0}", current shard: {1}'.format(shard, self.mheld_shards))
        with self.lock:
            if shard in self.mheld_shards:
                self.mheld_shards.remove(shard)
            if shard in self.mheart_shards:
                self.mheart_shards.remove(shard)
