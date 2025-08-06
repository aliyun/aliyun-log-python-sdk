#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

from .logresponse import LogResponse
from .util import Util
import json


class ListDeleteLogsTasksResponse(LogResponse):
    """ The response of the ListDeleteLogsTasks API from log.

    :type resp: byte
    :param resp: ListDeleteLogsTasksResponse HTTP response body

    :type header: dict
    :param header: ListDeleteLogsTasksResponse HTTP response header
    """

    def __init__(self, resp, header):
        LogResponse.__init__(self, header, resp)
        parsed_resp_data = {}

        if isinstance(resp, bytes):
            try:
                resp_str = resp.decode('utf-8')
                parsed_resp_data = json.loads(resp_str)
            except (UnicodeDecodeError, json.JSONDecodeError) as e:
                print("Warning: Failed to decode or parse 'resp' as JSON. Error: " + str(e))
                print("Raw bytes received: " + str(resp))
        elif isinstance(resp, dict):
            parsed_resp_data = resp

        self.count = parsed_resp_data.get('count', 0)
        self.total = parsed_resp_data.get('total', 0)
        self.task_list = parsed_resp_data.get('tasks', [])

    def is_completed(self):
        """ Check if the request is completed

        :return: bool, true if this request is completed
        """
        # 根据实际数据，当所有任务都完成时(进度为100%)表示完成
        if not self.task_list:
            return False
        return all(task.get('progress', 0) == 100 for task in self.task_list)

    def get_task_list(self):
        return self.task_list

    def get_count(self):
        return self.count

    def get_total(self):
        return self.total

    def log_print(self):
        print('ListDeleteLogsTasksResponse:')
        print('headers:', self.get_all_headers())
        print('count:', self.count)
        print('total:', self.total)
        print('task_list:', self.task_list)