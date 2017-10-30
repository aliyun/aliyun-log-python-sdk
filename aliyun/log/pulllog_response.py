#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

from aliyun.log.util import Util
from .log_logs_pb2 import LogGroupList
from .logresponse import LogResponse


class PullLogResponse(LogResponse):
    """ The response of the pull_logs API from log.
    
    :type header: dict
    :param header: PullLogResponse HTTP response header

    :type resp: dict
    :param resp: the HTTP response body
    """

    def __init__(self, resp, header):
        LogResponse.__init__(self, header)
        self.next_cursor = Util.convert_unicode_to_str(header["x-log-cursor"])
        self.log_count = int(header["x-log-count"])
        self.loggroup_list = LogGroupList()
        self._parse_loggroup_list(resp)
        self.loggroup_list_json = None

    def get_next_cursor(self):
        return self.next_cursor

    def get_log_count(self):
        return self.log_count

    def get_loggroup_count(self):
        return len(self.loggroup_list.LogGroups)

    def get_loggroup_json_list(self):
        if self.loggroup_list_json is None:
            self._transfer_to_json()
        return self.loggroup_list_json

    def get_loggroup_list(self):
        return self.loggroup_list

    def get_loggroup(self, index):
        if index < 0 or index >= len(self.loggroup_list.LogGroups):
            return None
        return self.loggroup_list.LogGroups[index]

    def log_print(self):
        print('PullLogResponse')
        print('next_cursor', self.next_cursor)
        print('log_count', self.log_count)
        print('headers:', self.get_all_headers())
        print('detail:', self.get_loggroup_json_list())

    def _parse_loggroup_list(self, data):
        self.loggroup_list.ParseFromString(data)
        # if not self.loggroup_list.ParseFromString(data):
        #     raise LogException('BadResponse', 'failed to parse data to LogGroupList')

    def _transfer_to_json(self):
        self.loggroup_list_json = []
        for logGroup in self.loggroup_list.LogGroups:
            items = []
            for log in logGroup.Logs:
                item = {'@lh_time': log.Time}
                for content in log.Contents:
                    item[content.Key] = content.Value
                items.append(item)
            log_items = {'topic': logGroup.Topic, 'source': logGroup.Source, 'logs': items}
            self.loggroup_list_json.append(log_items)
