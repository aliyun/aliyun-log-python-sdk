#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

from .logresponse import LogResponse
from .queriedlog import QueriedLog
from .logexception import LogException
import six


class GetContextLogsResponse(LogResponse):
    """ The response of the GetContextLog API from log.

    :type resp: dict
    :param resp: GetContextLogsResponse HTTP response body

    :type header: dict
    :param header: GetContextLogsResponse HTTP response header
    """

    def __init__(self, resp, header):
        LogResponse.__init__(self, header, resp)
        try:
            self.progress = resp["progress"]
            self.total_lines = resp["total_lines"]
            self.back_lines = resp["back_lines"]
            self.forward_lines = resp["forward_lines"]
            self.logs = []
            for data in resp["logs"]:
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
                               "Failed to parse GetContextLogResponse, \nheader: "
                               + str(header) + " \nBody:"
                               + str(resp) + " \nOther: " + str(ex),
                               resp_header=header,
                               resp_body=resp)

    def get_total_lines(self):
        """ Get log number from the response

        :return: int, log number
        """
        return self.total_lines

    def get_back_lines(self):
        """ Get log number backward from the response

        :return: int, log number
        """
        return self.back_lines

    def get_forward_lines(self):
        """ Get log number forward from the response

        :return: int, log number
        """
        return self.forward_lines

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
        print('GetContextLogsResponse:')
        print('headers:', self.get_all_headers())
        print('total_lines:', self.get_total_lines())
        print('back_lines:', self.get_back_lines())
        print('forward_lines:', self.get_forward_lines())
        print('progress:', self.progress)
        print('\nQueriedLog class:\n')
        for log in self.logs:
            log.log_print()
            print('\n')
