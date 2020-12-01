# -×- coding: utf-8 -*-

import logging
import time

from threading import Thread
from multiprocessing import RLock
from ..util import PrefixLoggerAdapter
import logging


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
        self.exception = None

        log_prefix = '/'.join([log_client.mproject, log_client.mlogstore,
                               log_client.mconsumer_group, log_client.mconsumer])

        extra = {
            "etl_context": """{
            "project": "%s", 
            "logstore": "%s", 
            "consumer_group": "%s", 
            "consumer": "%s"} """ % (log_client.mproject,
                                          log_client.mlogstore,
                                          log_client.mconsumer_group,
                                          log_client.mconsumer)
        }

        self.logger = PrefixLoggerAdapter("[{0}]".format(log_prefix), extra, logging.getLogger(__name__), {})

    def run(self):
        self.logger.debug('heart beat start', extra={"event_id": "heatbeat:run:enter"})
        while not self.shut_down_flag:
            try:
                response_shards = []
                last_heatbeat_time = time.time()

                with self.lock:
                    mheart_shards = self.mheart_shards[:]

                if self.log_client.heartbeat(mheart_shards, response_shards):
                    self.last_hearbeat_successed_unixtime = time.time()
                    self.logger.info('heart beat result: %s get: %s',
                                      mheart_shards, response_shards, extra={"event_id": "consumer_worker:heatbeat:status",
                                                    "extra_info_params":
                                                        """{"current_shards": "%s", "new_shards": "%s"}"""
                                                        % (mheart_shards, response_shards)})

                    if mheart_shards != response_shards:
                        current_set, response_set = set(
                            mheart_shards), set(response_shards)
                        add_set = response_set - current_set
                        remove_set = current_set - response_set
                        if any([add_set, remove_set]):
                            self.logger.info(
                                "shard reorganize, adding: %s, removing: %s",
                                add_set, remove_set, extra={"event_id": "consumer_worker:heatbeat:new_assignment",
                                                            "extra_info_params":
                                                                """{"add_shards": "%s", "remove_shards": "%s"}"""
                                                                % (list(add_set), list(remove_set)) })
                else:
                    if time.time() - self.last_hearbeat_successed_unixtime > \
                            (self.consumer_group_time_out + self.heartbeat_interval):
                        response_shards = []
                        self.logger.info(
                            "Heart beat timeout, automatic reset consumer held shards", extra={"event_id": "heatbeat:run:local_timeout_rest"})
                    else:
                        with self.lock:
                            response_shards = self.mheld_shards
                            self.logger.info(
                                "Heart beat failed, Keep the held shards unchanged", extra={"event_id": "heatbeat:run:fail_heatbeat_unchanged"})

                with self.lock:
                    self.mheart_shards = list(set(self.mheart_shards + response_shards))
                    self.mheld_shards = response_shards

                # default sleep for 2s from "LogHubConfig"
                time_to_sleep = self.heartbeat_interval - (time.time() - last_heatbeat_time)
                while time_to_sleep > 0 and not self.shut_down_flag:
                    time.sleep(min(time_to_sleep, 1))
                    time_to_sleep = self.heartbeat_interval - (time.time() - last_heatbeat_time)
            except Exception as e:
                self.logger.warning("fail to heat beat: %s", e, extra={"event_id": "heatbeat:run:fail_heatbeat", "reason": str(e)}, exc_info=True)
                self.exception = e
            else:
                self.exception = None

        self.logger.info('heart beat exit', extra={"event_id": "consumer_worker:exit:heatbeat"})

    def get_held_shards(self):
        """
        must copy to prevent race condition in multi-threads
        :return:
        """
        with self.lock:
            return self.mheld_shards[:]

    def shutdown(self):
        self.logger.debug('try to stop heart beat', extra={"event_id": "heatbeat:shutdown:start_shutdown"})
        self.shut_down_flag = True

    def remove_heart_shard(self, shard):
        self.logger.debug('try to remove shard "{0}", current shard: {1}'.format(shard, self.mheld_shards),
                         extra={"event_id": "heatbeat:remove_heart_shard",
                                "extra_info_params":
                                    """{"remove_shards": "%s", "current_held_shards": "%s"}"""
                                    % (shard, self.mheld_shards)}
                         )
        with self.lock:
            if shard in self.mheld_shards:
                self.mheld_shards.remove(shard)
            if shard in self.mheart_shards:
                self.mheart_shards.remove(shard)
