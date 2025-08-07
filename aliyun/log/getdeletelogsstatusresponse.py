#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

from .logresponse import LogResponse

class GetDeleteLogsStatusResponse(LogResponse):
    """ The response of the GetDeleteLogsStatus API from log.

    :type resp: dict
    :param resp: GetDeleteLogsStatusResponse HTTP response body

    :type header: dict
    :param header: GetDeleteLogsStatusResponse HTTP response header
    """

    def __init__(self, resp, header):
        LogResponse.__init__(self, header, resp)
        self.process = resp['progress']

    def is_completed(self):
        """ Check if the histogram is completed

        :return: bool, true if this histogram is completed
        """
        return self.process == 100.0

    def get_process(self):
        return self.process

    def log_print(self):
        print('GetDeleteLogsStatusResponse:')
        print('headers:', self.get_all_headers())
        print('progress:', self.process)

