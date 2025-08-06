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

    :type resp: bytes
    :param resp: DeleteLogsResponse HTTP response body

    :type header: dict
    :param header: DeleteLogsResponse HTTP response header
    """
    def __init__(self, resp, header):
        LogResponse.__init__(self, header, resp)
        parsed_resp_data = {}
        try:
            resp_str = resp.decode('utf-8')
            parsed_resp_data = json.loads(resp_str)
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            print("Warning: Failed to decode or parse 'resp' as JSON. Error: " + str(e))
            print("Raw bytes received: " + str(resp))
        self.taskid = parsed_resp_data.get('taskId')

    def is_completed(self):
        """ Check if the histogram is completed

        :return: bool, true if this histogram is completed
        """
        return self.progress == 'Complete'

    def get_taskid(self):
        return self.taskid

    def log_print(self):
        print('DeleteLogsResponse:')
        print('headers:', self.get_all_headers())
        print('taskid:', self.taskid)