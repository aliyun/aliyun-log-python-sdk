#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

from .logresponse import LogResponse
from .queriedlog import QueriedLog
from .logexception import LogException
import six
from .util import Util


class GetLogsResponse(LogResponse):
    """ The response of the GetLog API from log.
    
    :type resp: dict
    :param resp: GetLogsResponse HTTP response body
    
    :type header: dict
    :param header: GetLogsResponse HTTP response header
    """

    def __init__(self, resp, header):
        LogResponse.__init__(self, header, resp)
        try:
            self.progress = Util.h_v_t(header, 'x-log-progress')
            self.processed_rows = Util.h_v_td(header, 'x-log-processed-rows', '0')
            self.elapsed_mills = Util.h_v_td(header, 'x-log-elapsed-millisecond', '0')
            self.has_sql = Util.h_v_td(header, 'x-log-has-sql', 'False')
            self.where_query = Util.h_v_td(header, 'x-log-where-query', '')
            self.agg_query = Util.h_v_td(header, 'x-log-agg-query', '')
            self.cpu_sec = Util.h_v_td(header, 'x-log-cpu-sec', '0')
            self.cpu_cores = Util.h_v_td(header, 'x-log-cpu-cores', '0')
            self.logs = []
            for data in resp:
                contents = {}
                source = ""
                if "__source__" in data:
                    source = data['__source__']

                for key in six.iterkeys(data):
                    if key != '__time__' and key != '__source__':
                        contents[key] = data[key]
                self.logs.append(QueriedLog(data['__time__'], source, contents))
        except Exception as ex:
            raise LogException("InvalidResponse",
                               "Failed to parse GetLogResponse, \nheader: "
                               + str(header) + " \nBody:"
                               + str(resp) + " \nOther: " + str(ex),
                               resp_header=header,
                               resp_body=resp)

    def get_count(self):
        """ Get log number from the response
        
        :return: int, log number
        """
        return len(self.logs)

    def is_completed(self):
        """ Check if the get logs query is completed
        
        :return: bool, true if this logs query is completed
        """
        return self.progress == 'Complete'

    def get_logs(self):
        """ Get all logs from the response
        
        :return: QueriedLog list, all log data
        """
        return self.logs

    def get_processed_rows(self):
        """ Get processed rows from the response

        :return: processed_rows
        """
        return self.processed_rows

    def get_elapsed_mills(self):
        """ Get elapsed mills from the response

        :return: elapsed_mills
        """
        return self.elapsed_mills

    def get_has_sql(self):
        """ Get whether has sql from the response

        :return: has_sql, boolean
        """
        return self.has_sql

    def get_where_query(self):
        """ Get the Search part of "Search|Analysis"

        :return: where_query
        """
        return self.where_query

    def get_agg_query(self):
        """ Get the Analysis part of "Search|Analysis"

        :return: agg_query
        """
        return self.agg_query

    def get_cpu_sec(self):
        """ Get cpu seconds used from the response

        :return: cpu_sec
        """
        return self.cpu_sec

    def get_cpu_cores(self):
        """ Get cpu cores used from the response

        :return: cpu_cores
        """
        return self.cpu_cores

    def log_print(self):
        print('GetLogsResponse:')
        print('headers:', self.get_all_headers())
        print('count:', self.get_count())
        print('progress:', self.progress)
        print('\nQueriedLog class:\n')
        for log in self.logs:
            log.log_print()
            print('\n')

    def merge(self, response):
        if not isinstance(response, GetLogsResponse):
            raise ValueError("passed response is not a GetLogsResponse: " + str(type(response)))

        self.progress = response.progress
        self.logs.extend(response.get_logs())

        # update body
        self.body.extend(response.body)

        return self
