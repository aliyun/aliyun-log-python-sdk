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
    :param fromTime: the begin time or format of time in readable time like "%Y-%m-%d %H:%M:%S CST" e.g. "2018-01-02 12:12:10", also support human readable string, e.g. "1 hour ago", "now", "yesterday 0:0:0", refer to https://aliyun-log-cli.readthedocs.io/en/latest/tutorials/tutorial_human_readable_datetime.html
    
    :type toTime: int/string
    :param toTime: the end time or format of time in readable time like "%Y-%m-%d %H:%M:%S CST" e.g. "2018-01-02 12:12:10", also support human readable string, e.g. "1 hour ago", "now", "yesterday 0:0:0", refer to https://aliyun-log-cli.readthedocs.io/en/latest/tutorials/tutorial_human_readable_datetime.html

    :type topic: string
    :param topic: topic name of logs
    
    :type query: string
    :param query: user defined query
    """

    def __init__(self, project=None, logstore=None, fromTime=None, toTime=None, topic=None, query=None):
        LogRequest.__init__(self, project)
        self.logstore = logstore
        self.fromTime = parse_timestamp(fromTime)
        self.toTime = parse_timestamp(toTime)
        self.topic = topic
        self.query = query

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
