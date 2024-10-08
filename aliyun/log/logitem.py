#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

import time
import copy


class LogItem(object):
    """ LogItem used to present a log, it contains log time and multiple
    key/value pairs to present the log contents.

    :type timestamp: int with seconds as unit
    :param timestamp: time of the log item, the default time is the now time.

    :type contents: tuple(key-value) list
    :param contents: the data of the log item, including many (key,value) pairs.
    """

    def __init__(self, timestamp=None, time_nano_part=None, contents=None):
        nano_time = int(time.time() * 10**9)
        self.timestamp = int(timestamp) if timestamp else int(nano_time / 1000000000)
        # milliseconds
        if self.timestamp > 1e10:
            self.timestamp = int(self.timestamp / 1000.0)
        self.time_nano_part = int(time_nano_part) if time_nano_part else int(nano_time % 1000000000)
        self.contents = copy.deepcopy(contents) if contents else []

    def push_back(self, key, value):
        """ Append a key/value pair as a log content to the log

        :type key: string
        :param key: log content key

        :type value: string
        :param value: log content value
        """
        self.contents.append((key, value))

    def get_contents(self):
        """ Get log contents

        :return: tuple(key-value) list, log contents.
        """
        return self.contents

    def set_contents(self, contents):
        """ Set log contents

        :type contents: tuple(key-value) list
        :param contents: log contents (key-value pair list)
        """
        self.contents = copy.deepcopy(contents)

    def get_time(self):
        """ Get log time

        :return: int, log time
        """
        return self.timestamp

    def get_time_nano_part(self):
        """ Get log time nano part
        :return: int, log time
        """
        return self.time_nano_part

    def set_time(self, timestamp):
        """ Set log time

        :type timestamp: int
        :param timestamp: log time
        """
        # milliseconds
        if timestamp > 1e10:
            timestamp = timestamp / 1000.0
        self.timestamp = int(timestamp)

    def set_time_nano_part(self, time_nano_part):
        """ Set log time nano part
        :type time_nano_part: int
        :param time_nano_part: log time nano part
        """
        self.time_nano_part = int(time_nano_part)

    def log_print(self):
        print('time', self.timestamp)
        print('contents', self.contents)
