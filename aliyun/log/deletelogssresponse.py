#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

from .logresponse import LogResponse
from .histogram import Histogram
from .util import Util
import json


class DeleteLogsResponse(LogResponse):
    """ The response of the DeleteLogs API from log.

    :type resp: dict
    :param resp: DeleteLogsResponse HTTP response body

    :type header: dict
    :param header: DeleteLogsResponse HTTP response header
    """
    def __init__(self, resp, header):
        LogResponse.__init__(self, header, resp)
        self.taskid = resp.get('taskId')

    def get_taskid(self):
        return self.taskid

    def log_print(self):
        print('DeleteLogsResponse:')
        print('headers:', self.get_all_headers())
        print('taskid:', self.taskid)