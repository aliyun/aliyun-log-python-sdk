#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

from .logexception import LogException
from .logresponse import LogResponse
from .util import Util
from .util import base64_encodestring as b64e

from .log_logs_pb2 import LogGroupList
from .log_logs_raw_pb2 import LogGroupListRaw
import six

DEFAULT_DECODE_LIST = ('utf8',)


class PullLogResponse(LogResponse):
    """ The response of the pull_logs API from log.

    :type header: dict
    :param header: PullLogResponse HTTP response header

    :type resp: string
    :param resp: the HTTP response body
    """

    def __init__(self, resp, header):
        LogResponse.__init__(self, header, resp)
        self._is_bytes_type = None
        self.log_count = int(Util.h_v_t(header, "x-log-count"))
        self.raw_size = int(Util.h_v_t(header, 'x-log-bodyrawsize'))
        self.next_cursor = Util.convert_unicode_to_str(Util.h_v_t(header, "x-log-cursor"))
        self.raw_log_group_count_before_query = int(Util.h_v_td(self.headers, 'x-log-rawdatacount', '-1'))
        self.raw_size_before_query = int(Util.h_v_td(self.headers, 'x-log-rawdatasize', '-1'))
        self.loggroup_list = LogGroupList()
        if resp is not None:
            self._parse_loggroup_list(resp)
        self.loggroup_list_json = None
        self.flatten_logs_json = None
        self._body = None

    def get_body(self):
        if self._body is None:
            self._body = {"next_cursor": self.next_cursor,
                          "count": len(self.get_flatten_logs_json()),
                          "logs": self.get_flatten_logs_json()}
        return self._body

    @property
    def body(self):
        return self.get_body()

    @body.setter
    def body(self, value):
        self._body = value

    def get_next_cursor(self):
        return self.next_cursor

    def get_log_count(self):
        return len(self.get_flatten_logs_json())

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

    def get_raw_size(self):
        return self.raw_size

    def get_raw_log_group_count_before_query(self):
        return self.raw_log_group_count_before_query

    def get_raw_size_before_query(self):
        return self.raw_size_before_query

    def log_print(self):
        print('PullLogResponse')
        print('next_cursor', self.next_cursor)
        print('log_count', self.log_count)
        print('headers:', self.get_all_headers())
        print('detail:', self.get_loggroup_json_list())

    def _parse_loggroup_list(self, data):
        try:
            self.loggroup_list.ParseFromString(data)
        except Exception as ex:
            ex_second = None
            try:
                p = LogGroupListRaw()
                p.ParseFromString(data)
                self.loggroup_list = p
                self._is_bytes_type = True
                return
            except Exception as ex2:
                ex_second = ex2

            err = 'failed to parse data to LogGroupList: \n' \
                  + str(ex) + '\n' + str(ex_second) + '\nb64 raw data:\n' + b64e(data) \
                  + '\nheader:' + str(self.headers)
            raise LogException('BadResponse', err)

    def _transfer_to_json(self):
        self.loggroup_list_json = []
        for logGroup in self.loggroup_list.LogGroups:
            items = []
            tags = {}
            for tag in logGroup.LogTags:
                tags[tag.Key] = tag.Value

            for log in logGroup.Logs:
                item = {'@lh_time': log.Time}
                item['@time_ns'] = log.Time_ns
                for content in log.Contents:
                    item[content.Key] = content.Value
                items.append(item)
            log_items = {'topic': logGroup.Topic, 'source': logGroup.Source,
                         'logs': items,
                         'tags': tags}
            self.loggroup_list_json.append(log_items)


    @staticmethod
    def _b2u(content):
        if six.PY3 and isinstance(content, six.binary_type):
            if len(DEFAULT_DECODE_LIST) == 1:
                return content.decode(DEFAULT_DECODE_LIST[0], 'ignore')

            for d in DEFAULT_DECODE_LIST:
                try:
                    return content.decode(d)
                except Exception as ex:
                    continue

            # force to use utf8
            return content.decode('utf8', 'ignore')
        return content

    @staticmethod
    def get_log_count_from_group(loggroup_list):
        count = 0
        for logGroup in loggroup_list.LogGroups:
            for log in logGroup.Logs:
                count += 1
        return count

    @staticmethod
    def loggroups_to_flattern_list(loggroup_list, time_as_str=None, decode_bytes=None):
        flatten_logs_json = []
        for logGroup in loggroup_list.LogGroups:
            tags = {}
            for tag in logGroup.LogTags:
                tags[u"__tag__:{0}".format(tag.Key)] = tag.Value

            for log in logGroup.Logs:
                item = {u'__time__': six.text_type(log.Time) if time_as_str else log.Time,
                        u'__topic__': logGroup.Topic,
                        u'__source__': logGroup.Source}
                if log.Time_ns:
                    item[u'__time_ns_part__'] = log.Time_ns
                item.update(tags)
                for content in log.Contents:
                    item[PullLogResponse._b2u(content.Key) if decode_bytes else content.Key] = PullLogResponse._b2u(content.Value) if decode_bytes else content.Value
                flatten_logs_json.append(item)
        return flatten_logs_json

    def get_flatten_logs_json(self, time_as_str=None, decode_bytes=None):
        decode_bytes = decode_bytes or self._is_bytes_type

        if self.flatten_logs_json is None:
            self.flatten_logs_json = self.loggroups_to_flattern_list(self.loggroup_list, time_as_str=time_as_str,
                                                                     decode_bytes=decode_bytes)

        return self.flatten_logs_json

    def get_flatten_logs_json_auto(self):
        if self.flatten_logs_json is None:
            self.flatten_logs_json = self.loggroups_to_flattern_list(self.loggroup_list, time_as_str=True,
                                                                     decode_bytes=self._is_bytes_type)

        return self.flatten_logs_json
