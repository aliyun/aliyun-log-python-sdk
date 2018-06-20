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
