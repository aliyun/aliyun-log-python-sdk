#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

import time
import copy


class LogItem(object):
    """ LogItem used to present a log, it contains log time and multiple
    key/value pairs to present the log contents.
    
    :type timestamp: int
    :param timestamp: time of the log item, the default time is the now time.
    
    :type contents: tuple(key-value) list
    :param contents: the data of the log item, including many (key,value) pairs. 
    """

    def __init__(self, timestamp=None, contents=None):
        self.timestamp = int(timestamp) if timestamp else int(time.time())
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

    def set_time(self, timestamp):
        """ Set log time
        
        :type timestamp: int
        :param timestamp: log time
        """
        self.timestamp = int(timestamp)

    def log_print(self):
        print('time', self.timestamp)
        print('contents', self.contents)
