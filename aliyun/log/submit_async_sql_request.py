#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

from .logrequest import LogRequest
from .util import parse_timestamp


class SubmitAsyncSqlRequest(LogRequest):
    """ The request used to submit async SQL query.

    :type project: string
    :param project: project name

    :type logstore: string  
    :param logstore: logstore name

    :type query: string
    :param query: SQL query string

    :type fromTime: int/string
    :param fromTime: the begin time, or format of time in format "%Y-%m-%d %H:%M:%S" e.g. "2018-01-02 12:12:10"

    :type toTime: int/string
    :param toTime: the end time, or format of time in format "%Y-%m-%d %H:%M:%S" e.g. "2018-01-02 12:12:10"

    :type power_sql: bool
    :param power_sql: if power_sql is set to true, the query will run on enhanced sql mode

    :type allow_incomplete: bool
    :param allow_incomplete: if allow_incomplete is set to true, incomplete results are allowed

    :type max_run_time: int
    :param max_run_time: maximum execution time in seconds
    """

    def __init__(self, project=None, logstore=None, query=None, fromTime=None, toTime=None,
                 power_sql=False, allow_incomplete=True, max_run_time=None):
        LogRequest.__init__(self, project)
        self.logstore = logstore
        self.query = query
        self.fromTime = parse_timestamp(fromTime) if fromTime is not None else fromTime
        self.toTime = parse_timestamp(toTime) if toTime is not None else toTime
        self.power_sql = power_sql
        self.allow_incomplete = allow_incomplete
        self.max_run_time = max_run_time

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

    def get_query(self):
        """ Get SQL query string

        :return: string, SQL query string
        """
        return self.query

    def set_query(self, query):
        """ Set SQL query string

        :type query: string
        :param query: SQL query string
        """
        self.query = query

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

    def get_power_sql(self):
        """ Get power_sql flag

        :return: bool, power_sql flag
        """
        return self.power_sql

    def set_power_sql(self, power_sql):
        """ Set power_sql flag

        :type power_sql: bool
        :param power_sql: power_sql flag
        """
        self.power_sql = power_sql

    def get_allow_incomplete(self):
        """ Get allow_incomplete flag

        :return: bool, allow_incomplete flag
        """
        return self.allow_incomplete

    def set_allow_incomplete(self, allow_incomplete):
        """ Set allow_incomplete flag

        :type allow_incomplete: bool
        :param allow_incomplete: allow_incomplete flag
        """
        self.allow_incomplete = allow_incomplete

    def get_max_run_time(self):
        """ Get max_run_time

        :return: int, max_run_time
        """
        return self.max_run_time

    def set_max_run_time(self, max_run_time):
        """ Set max_run_time

        :type max_run_time: int
        :param max_run_time: maximum execution time in seconds
        """
        self.max_run_time = max_run_time
