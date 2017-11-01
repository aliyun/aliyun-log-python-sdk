# -*- coding: utf-8 -*-

import time

from .exceptions import CheckPointException
from ..logexception import LogException


class ConsumerCheckpointTracker(object):

    def __init__(self, loghub_client_adapter, consumer_name, shard_id):
        self.consumer_group_client = loghub_client_adapter
        self.consumer_name = consumer_name
        self.shard_id = shard_id
        self.last_check_time = time.time()
        self.cursor = ''
        self.temp_check_point = ''
        self.last_persistent_checkpoint = ''
        self.default_flush_check_point_interval = 60

    def set_cursor(self, cursor):
        self.cursor = cursor

    def get_cursor(self):
        return self.cursor

    def save_check_point(self, persistent, cursor=None):
        if cursor is not None:
            self.temp_check_point = cursor
        else:
            self.temp_check_point = self.cursor
        if persistent:
            self.flush_check_point()

    def set_memory_check_point(self, cursor):
        self.temp_check_point = cursor

    def set_persistent_check_point(self, cursor):
        self.last_persistent_checkpoint = cursor

    def flush_check_point(self):
        if self.temp_check_point != '' and self.temp_check_point != self.last_persistent_checkpoint:
            try:
                self.consumer_group_client.update_check_point(self.shard_id, self.consumer_name, self.temp_check_point)
                self.last_persistent_checkpoint = self.temp_check_point
            except LogException as e:
                raise CheckPointException("Failed to persistent the cursor to outside system, " +
                                          self.consumer_name + ", " + str(self.shard_id)
                                          + ", " + self.temp_check_point, e)

    def flush_check(self):
        current_time = time.time()
        if current_time > self.last_check_time + self.default_flush_check_point_interval:
            try:
                self.flush_check_point()
            except CheckPointException as e:
                print(e)
            self.last_check_time = current_time

    def get_check_point(self):
        return self.temp_check_point

