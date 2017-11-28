#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

import time

from .util import Util


class LogtailConfigDetail(object):
    """The common parts of logtail config
    :type config_name: string
    :param config_name: the config name

    :type logstore_name: string
    :param logstore_name: the logstore name for the config 

    :type endpoint: string
    :param endpoint: log data endpoint, it should base on the region of this project

    :type log_path: string
    :param log_path: the log file dir path

    :type file_pattern: string
    :param file_pattern: the log file name pattern, e.g *.LOG , access.log

    :type log_begin_regex: string
    :param log_begin_regex: the regular express to match the first line of a log

    :type topic_format: string
    :param topic_format: "none" or "group_topic"

    :type filter_keys: string list
    :param filter_keys: the keys used to filter logs, e.g ["key_1", "key_2"]

    :type filter_keys_reg: string list
    :param filter_keys_reg: the regex for filter_keys to filter the log, filter_keys_reg[i] is for filter_keys[i].
    The size of filter_keys_reg and filter_keys should be same.\
        If a log is matched only if the size of filter_keys is 0, or all the value of the related keys in filter_keys,
        match the regex set in filter_keys_reg

    :type logSample: string
    :param logSample: sample strings for the log, up to 1000 bytes

    """

    def __init__(self, config_name, logstore_name, endpoint, log_path, file_pattern, log_begin_regex, topic_format,
                 filter_keys, filter_keys_reg, logSample=''):
        self.config_name = config_name
        self.logstore_name = logstore_name
        self.endpoint = endpoint
        self.log_path = log_path
        self.file_pattern = file_pattern

        self.log_begin_regex = log_begin_regex
        self.topic_format = topic_format
        self.filter_keys = filter_keys
        self.filter_keys_reg = filter_keys_reg
        self.create_time = int(time.time())
        self.last_modify_time = int(time.time())
        self.logSample = logSample

    def set_create_time(self, create_time):
        self.create_time = create_time

    def set_last_modify_time(self, last_modify_time):
        self.last_modify_time = last_modify_time


class CommonRegLogConfigDetail(LogtailConfigDetail):
    """The logtail config for common_reg_log

    :type config_name: string
    :param config_name: the config name

    :type logstore_name: string
    :param logstore_name: the logstore name for the config 

    :type endpoint: string
    :param endpoint: log data endpoint, it should base on the region of this project

    :type log_path: string
    :param log_path: the log file dir path

    :type file_pattern: string
    :param file_pattern: the log file name pattern, e.g \*.LOG , access.log

    :type time_format: string
    :param time_format: the time format of the logs, e.g.  "%Y-%m-%d %M:%H:%S"

    :type log_begin_regex: string
    :param log_begin_regex: the regular express to match the first line of a log

    :type log_parse_regex: string
    :param log_parse_regex: the regular express to match a log, e.g (\d+-\d+\d+ \d+:\d+:\d+) (\S+) (.*)

    :type reg_keys: string list
    :param reg_keys: the key for every captured value in log_parse_reg, e.g ["time", "level", "message"]

    :type topic_format: string
    :param topic_format: "none" or "group_topic"

    :type filter_keys: string list
    :param filter_keys: the keys used to filter logs, e.g ["key_1", "key_2"]

    :type filter_keys_reg: string list
    :param filter_keys_reg: the regex for filter_keys to filter the log, filter_keys_reg[i] is for filter_keys[i]. The size of filter_keys_reg and filter_keys should be same. If a log is matched only if the size of filter_keys is 0, or all the value of the related keys in filter_keys, match the regex set in filter_keys_reg

    :type logSample: string
    :param logSample: sample strings for the log, up to 1000 bytes


    """

    def __init__(self, config_name, logstore_name, endpoint, log_path, file_pattern, time_format, log_begin_regex,
                 log_parse_regex, reg_keys,
                 topic_format="none", filter_keys=None, filter_keys_reg=None, logSample=''):
        if filter_keys is None:
            filter_keys = []
        if filter_keys_reg is None:
            filter_keys_reg = []

        LogtailConfigDetail.__init__(self, config_name, logstore_name, endpoint, log_path, file_pattern,
                                     log_begin_regex,
                                     topic_format, filter_keys, filter_keys_reg, logSample)

        self.time_format = time_format
        self.log_parse_regex = log_parse_regex
        self.keys = reg_keys

    def to_json(self):
        json_value = {"configName": self.config_name, "inputType": "file"}

        # add log sample
        if self.logSample:
            json_value["logSample"] = self.logSample

        detail = {'logType': 'common_reg_log', 'logPath': self.log_path, 'filePattern': self.file_pattern,
                  'localStorage': True, 'timeFormat': self.time_format, 'logBeginRegex': self.log_begin_regex,
                  'regex': self.log_parse_regex, 'key': self.keys, 'filterKey': self.filter_keys,
                  'filterRegex': self.filter_keys_reg, 'topicFormat': self.topic_format}
        json_value["inputDetail"] = detail
        json_value["outputType"] = "LogService"
        output_detail = {}
        if self.endpoint is not None:
            output_detail["endpoint"] = self.endpoint
        output_detail["logstoreName"] = self.logstore_name
        json_value['outputDetail'] = output_detail
        return json_value


class ApsaraLogConfigDetail(LogtailConfigDetail):
    """The logtail config for apsara_log
    :type config_name: string
    :param config_name: the config name

    :type logstore_name: string
    :param logstore_name: the logstore name for the config 

    :type endpoint: string
    :param endpoint: log data endpoint, it should base on the region of this project

    :type log_path: string
    :param log_path: the log file dir path

    :type file_pattern: string
    :param file_pattern: the log file name pattern, e.g *.LOG , access.log

    :type log_begin_regex: string
    :param log_begin_regex: the regular express to match the first line of a log

    :type topic_format: string
    :param topic_format: "none" or "group_topic"

    :type filter_keys: string list
    :param filter_keys: the keys used to filter logs, e.g ["key_1", "key_2"]

    :type filter_keys_reg: string list
    :param filter_keys_reg: the regex for filter_keys to filter the log, filter_keys_reg[i] is for filter_keys[i]. The size of filter_keys_reg and filter_keys should be same. If a log is matched only if the size of filter_keys is 0, or all the value of the related keys in filter_keys, match the regex set in filter_keys_reg
    """

    def __init__(self, config_name, logstore_name, endpoint, log_path, file_pattern,
                 log_begin_regex=r'\[\d+-\d+-\d+ \d+:\d+:\d+\.\d+.*', topic_format="none", filter_keys=None,
                 filter_keys_reg=None, logSample=''):
        if filter_keys_reg is None:
            filter_keys_reg = []
        if filter_keys is None:
            filter_keys = []
        LogtailConfigDetail.__init__(self, config_name, logstore_name, endpoint, log_path, file_pattern,
                                     log_begin_regex,
                                     topic_format, filter_keys, filter_keys_reg, logSample)

    def to_json(self):
        json_value = {"configName": self.config_name, "inputType": "file"}
        # add log sample
        if self.logSample:
            json_value["logSample"] = self.logSample

        detail = {'logType': 'apsara_log', 'logPath': self.log_path, 'filePattern': self.file_pattern,
                  'localStorage': True, 'logBeginRegex': self.log_begin_regex, 'timeFormat': '',
                  'filterKey': self.filter_keys, 'filterRegex': self.filter_keys_reg, 'topicFormat': self.topic_format}
        json_value["inputDetail"] = detail
        json_value["outputType"] = "log"
        output_detail = {}
        if self.endpoint is not None:
            output_detail["endpoint"] = self.endpoint
        output_detail["logstoreName"] = self.logstore_name
        json_value['outputDetail'] = output_detail
        return json_value


class LogtailConfigHelper(object):
    """
    A helper to generate logtail config object from dict object (loaded from json)
    """

    @staticmethod
    def generate_common_reg_log_config(json_value):
        """Generate common logtail config from loaded json value

        :param json_value:
        :return:
        """
        input_detail = json_value['inputDetail']
        output_detail = json_value['outputDetail']
        logSample = json_value.get('logSample', '')
        config_name = json_value['configName']
        logstore_name = output_detail['logstoreName']
        endpoint = output_detail.get('endpoint', None)

        log_path = input_detail['logPath']
        file_pattern = input_detail['filePattern']

        time_format = input_detail['timeFormat']
        log_begin_regex = input_detail.get('logBeginRegex', '')
        log_parse_regex = input_detail['regex']
        reg_keys = input_detail['key']
        topic_format = input_detail['topicFormat']
        filter_keys = input_detail['filterKey']
        filter_keys_reg = input_detail['filterRegex']

        config = CommonRegLogConfigDetail(config_name, logstore_name, endpoint, log_path, file_pattern, time_format,
                                          log_begin_regex, log_parse_regex, reg_keys,
                                          topic_format, filter_keys, filter_keys_reg, logSample)
        return config

    @staticmethod
    def generate_apsara_log_config(json_value):
        """Generate apsara logtail config from loaded json value

        :param json_value:
        :return:
        """
        input_detail = json_value['inputDetail']
        output_detail = json_value['outputDetail']
        config_name = json_value['configName']
        logSample = json_value.get('logSample', '')

        logstore_name = output_detail['logstoreName']
        endpoint = output_detail.get('endpoint')
        log_path = input_detail['logPath']
        file_pattern = input_detail['filePattern']

        log_begin_regex = input_detail.get('logBeginRegex', '')
        topic_format = input_detail['topicFormat']
        filter_keys = input_detail['filterKey']
        filter_keys_reg = input_detail['filterRegex']

        config = ApsaraLogConfigDetail(config_name, logstore_name, endpoint, log_path, file_pattern,
                                       log_begin_regex, topic_format, filter_keys, filter_keys_reg, logSample)
        return config

    @staticmethod
    def generate_logtail_config(json_value):
        """Generate logtail config from loaded json value

        :param json_value:
        :return:
        """
        if json_value['inputDetail']['logType'] == 'common_reg_log':
            return LogtailConfigHelper.generate_common_reg_log_config(json_value)
        return LogtailConfigHelper.generate_apsara_log_config(json_value)
