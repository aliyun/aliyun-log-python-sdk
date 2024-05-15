#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

from .logrequest import LogRequest
from .util import parse_timestamp

class GetHistogramsRequest(LogRequest):
    """ The request used to get histograms of a query from log.

    :type project: string
    :param project: project name

    :type logstore: string
    :param logstore: logstore name

    :type fromTime: int/string
    :param fromTime: the begin time or format of time in readable time like "%Y-%m-%d %H:%M:%S<time_zone>" e.g. "2018-01-02 12:12:10+8:00" e.g. "2018-01-02 12:12:10", also support human readable string, e.g. "1 hour ago", "now", "yesterday 0:0:0", refer to https://aliyun-log-cli.readthedocs.io/en/latest/tutorials/tutorial_human_readable_datetime.html

    :type toTime: int/string
    :param toTime: the end time or format of time in readable time like "%Y-%m-%d %H:%M:%S<time_zone>" e.g. "2018-01-02 12:12:10+8:00" e.g. "2018-01-02 12:12:10", also support human readable string, e.g. "1 hour ago", "now", "yesterday 0:0:0", refer to https://aliyun-log-cli.readthedocs.io/en/latest/tutorials/tutorial_human_readable_datetime.html

    :type topic: string
    :param topic: topic name of logs

    :type query: string
    :param query: user defined query

    :type from_time_nano_part: int
    :param from_time_nano_part: nano part of query begin time

    :type to_time_nano_part: int
    :param to_time_nano_part: nano part of query end time

    :type shard_id: int
    :param shard_id: specific shard id,-1 means all shard
    """

    def __init__(self, project=None, logstore=None, fromTime=None, toTime=None, topic=None, query=None, accurate_query=True, from_time_nano_part=0, to_time_nano_part=0, shard_id=-1):
        LogRequest.__init__(self, project)
        self.logstore = logstore
        self.fromTime = parse_timestamp(fromTime)
        self.toTime = parse_timestamp(toTime)
        self.topic = topic
        self.query = query
        self.accurate_query = accurate_query
        self.from_time_nano_part = from_time_nano_part
        self.to_time_nano_part = to_time_nano_part
        self.shard_id = shard_id

    def get_logstore(self):
        """ Get logstore name

        :return: string, logstore name.
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

    def get_from(self):
        """ Get begin time

        :return: int, begin time
        """
        return self.fromTime

    def set_from(self, fromTime):
        """ Set begin time

        :type fromTime: int
        :param fromTime: begin time
        """
        self.fromTime = fromTime

    def get_to(self):
        """ Get end time

        :return: int, end time
        """
        return self.toTime

    def set_to(self, toTime):
        """ Set end time

        :type toTime: int
        :param toTime: end time
        """
        self.toTime = toTime

    def get_query(self):
        """ Get user defined query

        :return: string, user defined query
        """
        return self.query

    def set_query(self, query):
        """ Set user defined query

        :type query: string
        :param query: user defined query
        """
        self.query = query

    def get_accurate_query(self):
        """ Get user defined accurate_query
        :return: string, user defined accurate_query
        """
        return self.accurate_query

    def set_accurate_query(self, accurate_query):
        """ Set user defined accurate_query
        :type accurate_query: string
        :param accurate_query: user defined accurate_query
        """
        self.accurate_query = accurate_query

    def get_from_time_nano_part(self):
        """ Get request from_time_nano_part

        :return: int, from_time_nano_part
        """
        return self.from_time_nano_part

    def set_from_time_nano_part(self, from_time_nano_part):
        """ Set request from_time_nano_part

        :type from_time_nano_part: int
        :param from_time_nano_part: from_time_nano_part part of query begin time
        """
        self.from_time_nano_part = from_time_nano_part

    def get_to_time_nano_part(self):
        """ Get request to_time_nano_part

        :return: int, to_time_nano_part
        """
        return self.to_time_nano_part

    def set_to_time_nano_part(self, to_time_nano_part):
        """ Set request to_time_nano_part

        :type to_time_nano_part: int
        :param to_time_nano_part: to_time_nano_part part of query end time
        """
        self.to_time_nano_part = to_time_nano_part

    def set_shard_id(self, shard_id):
        """ Set request shard_id

        :type shard_id: int
        :param shard_id: user defined shard_id
        """
        self.shard_id = shard_id

    def get_shard_id(self):
        """ Get request shard_id

        :return: int, shard_id
        """
        return self.shard_id
