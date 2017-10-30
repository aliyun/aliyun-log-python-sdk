#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

from .logrequest import LogRequest


class GetLogsRequest(LogRequest):
    """ The request used to get logs by a query from log.
    
    :type project: string
    :param project: project name
    
    :type logstore: string
    :param logstore: logstore name
    
    :type fromTime: int
    :param fromTime: the begin time
    
    :type toTime: int
    :param toTime: the end time

    :type topic: string
    :param topic: topic name of logs
    
    :type query: string
    :param query: user defined query
    
    :type line: int
    :param line: max line number of return logs
    
    :type offset: int
    :param offset: line offset of return logs
    
    :type reverse: bool
    :param reverse: if reverse is set to true, the query will return the latest logs first
    """

    def __init__(self, project=None, logstore=None, fromTime=None, toTime=None, topic=None,
                 query=None, line=None, offset=None, reverse=None):
        LogRequest.__init__(self, project)
        self.logstore = logstore
        self.fromTime = fromTime
        self.toTime = toTime
        self.topic = topic
        self.query = query
        self.line = line
        self.offset = offset
        self.reverse = reverse

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

    def get_line(self):
        """ Get max line number of return logs
        
        :return: int, max line number of return logs
        """
        return self.line

    def set_line(self, line):
        """ Set max line number of return logs
        
        :type line: int
        :param line: max line number of return logs
        """
        self.line = line

    def get_offset(self):
        """ Get line offset of return logs
        
        :return: int, line offset of return logs
        """
        return self.offset

    def set_offset(self, offset):
        """ Set line offset of return logs
        
        :type offset: int
        :param offset: line offset of return logs
        """
        self.offset = offset

    def get_reverse(self):
        """ Get request reverse flag
        
        :return: bool, reverse flag
        """
        return self.reverse

    def set_reverse(self, reverse):
        """ Set request reverse flag
        
        :type reverse: bool
        :param reverse: reverse flag
        """
        self.reverse = reverse
