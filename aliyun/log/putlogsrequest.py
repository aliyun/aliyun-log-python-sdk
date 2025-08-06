#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

from .logrequest import LogRequest


class PutLogsRequest(LogRequest):
    """ The request used to send data to log.

    :type project: string
    :param project: project name

    :type logstore: string
    :param logstore: logstore name

    :type topic: string
    :param topic: topic name

    :type source: string
    :param source: source of the logs

    :type logitems: list<LogItem>
    :param logitems: log data

    :type hashKey: String
    :param hashKey: put data with set hash, the data will be send to shard whose range contains the hashKey

    :type compress: bool
    :param compress: if need to compress the logs, default is True

    :type logtags: list
    :param logtags: list of key:value tag pair , [(tag_key_1,tag_value_1) , (tag_key_2,tag_value_2)]
    
    :type compress_type: String
    :param compress_type: compress_type, eg lz4, zstd, default is lz4. To use zstd, pip install zstd.

    """

    def __init__(self, project=None, logstore=None, topic=None, source=None, logitems=None, hashKey=None,
                 compress=True, logtags=None, compress_type=None):
        LogRequest.__init__(self, project)
        self.logstore = logstore
        self.topic = topic
        self.source = source
        self.logitems = logitems
        self.hashkey = hashKey
        self.compress = compress
        self.logtags = logtags
        self.compress_type = compress_type

    def get_compress_type(self):
        return self.compress_type
    
    def set_compress_type(self, compress_type):
        self.compress_type = compress_type

    def get_compress(self):
        return self.compress

    def set_compress(self, compress):
        self.compress = compress

    def get_logstore(self):
        """ Get logstore name

        :return: string, logstore name
        """
        return self.logstore if self.logstore else ''

    def set_logstore(self, logstore):
        """ Set logstore name

        :type logstore: string
        :param logstore: logstore name
        """
        self.logstore = logstore

    def get_topic(self):
        """ Get topic name

        :return: string, topic name
        """
        return self.topic if self.topic else ''

    def set_topic(self, topic):
        """ Set topic name

        :type topic: string
        :param topic: topic name
        """
        self.topic = topic

    def get_source(self):
        """ Get log source

        :return: string, log source
        """
        return self.source

    def set_source(self, source):
        """ Set log source

        :type source: string
        :param source: log source
        """
        self.source = source

    def get_log_items(self):
        """ Get all the log data

        :return: LogItem list, log data
        """
        return self.logitems

    def set_log_items(self, logitems):
        """ Set the log data

        :type logitems: LogItem list
        :param logitems: log data
        """
        self.logitems = logitems

    def get_log_tags(self):
        """ Get all the log tags

        :return: Logtags list, log data
        """
        return self.logtags

    def set_log_tags(self, logtags):
        """ Set the log tags

        :type logtags: logtags list
        :param logtags: log tags
        """
        self.logtags = logtags

    def set_hash_key(self, hashKey):
        self.hashkey = hashKey

    def get_hash_key(self):
        return self.hashkey
