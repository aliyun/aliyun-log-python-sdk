#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

import time

from .util import Util
import copy
import warnings
from enum import Enum
from .logexception import LogException
import logging

__all__ = ['PluginConfigDetail', 'SeperatorFileConfigDetail', 'SimpleFileConfigDetail', 'FullRegFileConfigDetail',
           'JsonFileConfigDetail', 'ApsaraFileConfigDetail', 'SyslogConfigDetail',
           'LogtailConfigGenerator', 'CommonRegLogConfigDetail']

logger = logging.getLogger(__name__)


class LogtailInputType(Enum):
    FILE = 'file'
    SYSLOG = 'syslog'
    PLUGIN = 'plugin'


class LogtailType(Enum):
    JSON = 'json_log'
    FULL_REGEX = 'common_reg_log'
    SEPARATOR = 'delimiter_log'
    APSARA = "apsara_log"


class LogtailConfigDetail(object):
    """The common parts of logtail config
    :type config_name: string
    :param config_name: the config name

    :type logstore_name: string
    :param logstore_name: the logstore name for the config

    :type endpoint: string
    :param endpoint: deprecated, set it as empty

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

    :type log_type: string
    :param log_type: common_reg_log, delimiter_log, apsara_log etc.

    :type extended_items: dict
    :param extended_items: extended items in dict format e.g. enableRawLog etc. refer to wiki page.
    https://help.aliyun.com/document_detail/29042.html?spm=5176.doc28997.6.744.C583Jg

    """

    def __init__(self, config_name, logstore_name, endpoint, log_path, file_pattern, log_begin_regex, topic_format,
                 filter_keys, filter_keys_reg, logSample='', log_type='common_reg_log', **extended_items):
        logger.warning("aliyun.log.LogtailConfigDetail is deprecated and will be removed in future version.")

        self.config_name = config_name
        self.logstore_name = logstore_name
        self.endpoint = endpoint or ''
        self.log_path = log_path
        self.file_pattern = file_pattern

        self.log_begin_regex = log_begin_regex
        self.topic_format = topic_format
        self.filter_keys = filter_keys
        self.filter_keys_reg = filter_keys_reg
        self.create_time = int(time.time())
        self.last_modify_time = int(time.time())
        self.logSample = logSample
        self.log_type = log_type or 'common_reg_log'
        self.extended_items = extended_items

    def set_create_time(self, create_time):
        self.create_time = create_time

    def set_last_modify_time(self, last_modify_time):
        self.last_modify_time = last_modify_time

    @staticmethod
    def from_json(json_value):
        logger.warning("aliyun.log.LogtailConfigDetail is deprecated and will be removed in future version.")
        return LogtailConfigHelper.generate_logtail_config(json_value)


class CommonRegLogConfigDetail(LogtailConfigDetail):
    """The logtail config for common_reg_log

    :type config_name: string
    :param config_name: the config name

    :type logstore_name: string
    :param logstore_name: the logstore name for the config

    :type endpoint: string
    :param endpoint: log data endpoint, deprecated, set it as empty

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

    :type log_type: string
    :param log_type: common_reg_log, delimiter_log, apsara_log etc.

    :type extended_items: dict
    :param extended_items: extended items in dict format e.g. enableRawLog etc. refer to wiki page.
    https://help.aliyun.com/document_detail/29042.html?spm=5176.doc28997.6.744.C583Jg

    """

    def __init__(self, config_name, logstore_name, endpoint, log_path, file_pattern, time_format, log_begin_regex,
                 log_parse_regex, reg_keys,
                 topic_format="none", filter_keys=None, filter_keys_reg=None, logSample='',
                 log_type='common_reg_log', **extended_items):
        logger.warning("aliyun.log.CommonRegLogConfigDetail is deprecated and will be removed in future version."
                      "Use ConfigDetailBase based class instead")

        if filter_keys is None:
            filter_keys = []
        if filter_keys_reg is None:
            filter_keys_reg = []

        LogtailConfigDetail.__init__(self, config_name, logstore_name, endpoint, log_path, file_pattern,
                                     log_begin_regex,
                                     topic_format, filter_keys, filter_keys_reg, logSample, log_type, **extended_items)

        self.time_format = time_format
        self.log_parse_regex = log_parse_regex
        self.keys = reg_keys

    def to_json(self):
        json_value = {"configName": self.config_name, "inputType": "file"}

        # add log sample
        if self.logSample:
            json_value["logSample"] = self.logSample

        detail = {'logType': self.log_type, 'logPath': self.log_path, 'filePattern': self.file_pattern,
                  'localStorage': True, 'timeFormat': self.time_format, 'logBeginRegex': self.log_begin_regex,
                  'regex': self.log_parse_regex, 'key': self.keys, 'filterKey': self.filter_keys,
                  'filterRegex': self.filter_keys_reg, 'topicFormat': self.topic_format}
        detail.update(self.extended_items)
        json_value["inputDetail"] = detail
        json_value["outputType"] = "LogService"
        output_detail = {}
        if self.endpoint:
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
    :param endpoint: deprecated, set it as empty

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

    :type extended_items: dict
    :param extended_items: extended items in dict format e.g. enableRawLog etc. refer to wiki page.
    https://help.aliyun.com/document_detail/29042.html?spm=5176.doc28997.6.744.C583Jg

    """

    def __init__(self, config_name, logstore_name, endpoint, log_path, file_pattern,
                 log_begin_regex=r'\[\d+-\d+-\d+ \d+:\d+:\d+\.\d+.*', topic_format="none", filter_keys=None,
                 filter_keys_reg=None, logSample='', **extended_items):
        logger.warning("aliyun.log.ApsaraLogConfigDetail is deprecated and will be removed in future version. "
                      "Use ConfigDetailBase based class instead")

        if filter_keys_reg is None:
            filter_keys_reg = []
        if filter_keys is None:
            filter_keys = []
        LogtailConfigDetail.__init__(self, config_name, logstore_name, endpoint, log_path, file_pattern,
                                     log_begin_regex,
                                     topic_format, filter_keys, filter_keys_reg, logSample,
                                     'apsara_log', **extended_items)

    def to_json(self):
        json_value = {"configName": self.config_name, "inputType": "file"}
        # add log sample
        if self.logSample:
            json_value["logSample"] = self.logSample

        detail = {'logType': 'apsara_log', 'logPath': self.log_path, 'filePattern': self.file_pattern,
                  'localStorage': True, 'logBeginRegex': self.log_begin_regex, 'timeFormat': '',
                  'filterKey': self.filter_keys, 'filterRegex': self.filter_keys_reg, 'topicFormat': self.topic_format}
        detail.update(self.extended_items)  # add more extended items
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
        input_detail = copy.deepcopy(json_value['inputDetail'])
        output_detail = json_value['outputDetail']
        logSample = json_value.get('logSample', '')
        config_name = json_value['configName']
        logstore_name = output_detail['logstoreName']
        endpoint = output_detail.get('endpoint', '')

        log_path = input_detail['logPath']
        file_pattern = input_detail['filePattern']
        time_format = input_detail['timeFormat']
        log_begin_regex = input_detail.get('logBeginRegex', '')
        log_parse_regex = input_detail.get('regex', '')
        reg_keys = input_detail['key']
        topic_format = input_detail['topicFormat']
        filter_keys = input_detail['filterKey']
        filter_keys_reg = input_detail['filterRegex']
        log_type = input_detail.get('logType')

        for item in ('logPath', 'filePattern', 'timeFormat', 'logBeginRegex', 'regex', 'key',
                     'topicFormat', 'filterKey', 'filterRegex', 'logType'):
            if item in input_detail:
                del input_detail[item]

        config = CommonRegLogConfigDetail(config_name, logstore_name, endpoint, log_path, file_pattern, time_format,
                                          log_begin_regex, log_parse_regex, reg_keys,
                                          topic_format, filter_keys, filter_keys_reg, logSample,
                                          log_type, **input_detail)
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
        endpoint = output_detail.get('endpoint', '')
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
        logger.warning("aliyun.log.LogtailConfigHelper is deprecated and will be removed in future version."
                      "Use LogtailConfigGenerator instead")

        if json_value['inputDetail']['logType'] == 'apsara_log':
            return LogtailConfigHelper.generate_apsara_log_config(json_value)
        return LogtailConfigHelper.generate_common_reg_log_config(json_value)


class ConfigDetailBase(object):
    MANDATORY_FIELDS_ROOT = ["configName", "inputType", "inputDetail"]
    MANDATORY_FIELDS_DETAIL = []
    DEFAULT_DETAIL_FIELDS = [('localStorage', True)]

    def __init__(self, logstoreName, configName, inputType, createTime=None, modifyTime=None, logSample=None,
                 **input_detail):
        self.value = {
            "configName": configName,
            "logSample": logSample,
            "createTime": createTime,
            "modifyTime": modifyTime,
            "inputDetail": input_detail,
            "inputType": inputType,
            "outputDetail": {
                "logstoreName": logstoreName
            },
            "outputType": "LogService"
        }

        # clean up none items
        self.__clean_up_non_items()

        # add default ones
        for k, v in self.DEFAULT_DETAIL_FIELDS:
            if k not in self.value["inputDetail"]:
                self.value["inputDetail"][k] = v

    @property
    def config_name(self):
        return self.value["configName"]

    @config_name.setter
    def config_name(self, value):
        self.value["configName"] = value

    @property
    def logstore_name(self):
        return self.value["outputDetail"]["logstoreName"]

    @logstore_name.setter
    def logstore_name(self, value):
        self.value["outputDetail"]["logstoreName"] = value

    def __clean_up_non_items(self):
        none_items = [k for k, v in self.value.items() if v is None]
        for k in none_items:
            del self.value[k]
        none_detail_items = [k for k, v in self.value["inputDetail"].items() if v is None]
        for k in none_detail_items:
            del self.value["inputDetail"][k]

    @classmethod
    def from_json(cls, json_value):
        for item in cls.MANDATORY_FIELDS_ROOT:
            if item not in json_value:
                raise ValueError('item "{0}" not in json value'.format(item))

        for item in cls.MANDATORY_FIELDS_DETAIL:
            if item not in json_value["inputDetail"]:
                raise ValueError('item "{0}" not in json value "inputDetail"'.format(item))

        logstore_name = json_value["outputDetail"]["logstoreName"]
        config_name = json_value["configName"]
        input_type = json_value["inputType"]
        create_time = json_value.get("createTime", None)
        modify_time = json_value.get("modifyTime", None)
        log_sample = json_value.get("logSample", None)
        input_detail = json_value["inputDetail"]

        if cls == ConfigDetailBase:
            return cls(logstoreName=logstore_name, configName=config_name,
                       input_type=input_type,
                       createTime=create_time, modifyTime=modify_time,
                       logSample=log_sample, **input_detail)

        return cls(logstoreName=logstore_name, configName=config_name,
                   createTime=create_time, modifyTime=modify_time,
                   logSample=log_sample, **input_detail)

    def to_json(self):
        return self.value


class PluginConfigDetail(ConfigDetailBase):
    """The logtail config for simple mode

    :type logstoreName: string
    :param logstoreName: the logstore name

    :type configName: string
    :param configName: the config name

    :type logPath: string
    :param logPath: folder of log path /apsara/nuwa/

    :type filePattern: string
    :param filePattern: file path, e.g. *.log, it will be /apsara/nuwa/.../*.log

    :type localStorage: bool
    :param localStorage: if use local cache 1GB when logtail is offline. default is True.

    :type enableRawLog: bool
    :param enableRawLog: if upload raw data in content, default is False

    :type topicFormat: string
    :param topicFormat: "none", "group_topic" or regex to extract value from file path e.g. "/test/(\w+).log" will extract each file as topic, default is "none"

    :type fileEncoding: string
    :param fileEncoding: "utf8" or "gbk" so far

    :type maxDepth: int
    :param maxDepth: max depth of folder to scan, by default its 100, 0 means just scan the root folder

    :type preserve: bool
    :param preserve: if preserve time-out, by default is False, 30 min time-out if set it as True

    :type preserveDepth: int
    :param preserveDepth: time-out folder depth. 1-3

    :type filterKey: string list
    :param filterKey: only keep log which match the keys. e.g. ["city", "location"] will only scan files math the two fields

    :type filterRegex: string list
    :param filterRegex: matched value for filterKey, e.g. ["shanghai|beijing|nanjing", "east"] note, it's regex value list

    :type createTime: int
    :param createTime: timestamp of created, only useful when getting data from REST

    :type modifyTime: int
    :param modifyTime: timestamp of last modified time, only useful when getting data from REST

    :type extended_items: dict
    :param extended_items: extended items

    """
    MANDATORY_FIELDS_DETAIL = ConfigDetailBase.MANDATORY_FIELDS_DETAIL + ['plugin']
    DEFAULT_DETAIL_FIELDS = ConfigDetailBase.DEFAULT_DETAIL_FIELDS

    def __init__(self, logstoreName, configName, plugin, **extended_items):
        input_detail = {
            "plugin": plugin
        }
        input_detail.update(extended_items)

        ConfigDetailBase.__init__(self, logstoreName, configName, "plugin", **input_detail)


class SeperatorFileConfigDetail(ConfigDetailBase):
    """The logtail config for separator mode

    :type logstoreName: string
    :param logstoreName: the logstore name

    :type configName: string
    :param configName: the config name

    :type logPath: string
    :param logPath: folder of log path /apsara/nuwa/

    :type filePattern: string
    :param filePattern: file path, e.g. *.log, it will be /apsara/nuwa/.../*.log

    :type logSample: string
    :param logSample: log sample. e.g. shanghai|2000|east

    :type separator: string
    :param separator: '\t' for tab, ' ' for space, '|',  up to 3 chars like "&&&" or "||" etc.

    :type key: string list
    :param key: keys to map the fields like ["city", "population", "location"]

    :type timeKey: string
    :param timeKey: one key name in `key` to set the time or set it None to use system time.

    :type timeFormat: string
    :param timeFormat: whe timeKey is not None, set its format, refer to https://help.aliyun.com/document_detail/28980.html?spm=5176.2020520112.113.4.2243b18eHkxdNB

    :type localStorage: bool
    :param localStorage: if use local cache 1GB when logtail is offline. default is True.

    :type enableRawLog: bool
    :param enableRawLog: if upload raw data in content, default is False

    :type topicFormat: string
    :param topicFormat: "none", "group_topic" or regex to extract value from file path e.g. "/test/(\w+).log" will extract each file as topic, default is "none"

    :type fileEncoding: string
    :param fileEncoding: "utf8" or "gbk" so far

    :type maxDepth: int
    :param maxDepth: max depth of folder to scan, by default its 100, 0 means just scan the root folder

    :type preserve: bool
    :param preserve: if preserve time-out, by default is False, 30 min time-out if set it as True

    :type preserveDepth: int
    :param preserveDepth: time-out folder depth. 1-3

    :type filterKey: string list
    :param filterKey: only keep log which match the keys. e.g. ["city", "location"] will only scan files math the two fields

    :type filterRegex: string list
    :param filterRegex: matched value for filterKey, e.g. ["shanghai|beijing|nanjing", "east"] note, it's regex value list

    :type createTime: int
    :param createTime: timestamp of created, only useful when getting data from REST

    :type modifyTime: int
    :param modifyTime: timestamp of last modified time, only useful when getting data from REST

    :type extended_items: dict
    :param extended_items: extended items

    """
    MANDATORY_FIELDS_ROOT = ConfigDetailBase.MANDATORY_FIELDS_ROOT + ["logSample"]
    MANDATORY_FIELDS_DETAIL = ConfigDetailBase.MANDATORY_FIELDS_DETAIL \
                              + ["logPath", "filePattern", "separator", "key"]
    DEFAULT_DETAIL_FIELDS = [("logType", "delimiter_log"), ("localStorage", True),
                             ("timeFormat", ''), ("topicFormat", "none"), ("autoExtend", True)]

    def __init__(self, logstoreName, configName, logPath, filePattern, logSample, separator, key,
                 timeKey='', timeFormat=None, localStorage=None, enableRawLog=None, topicFormat=None,
                 fileEncoding=None, maxDepth=None, preserve=None, preserveDepth=None, filterKey=None,
                 filterRegex=None, createTime=None, modifyTime=None, **extended_items):
        input_detail = {
            "logPath": logPath,
            "filePattern": filePattern,
            "separator": separator,
            "key": key,
            "timeFormat": timeFormat,
            "timeKey": timeKey,
            "localStorage": localStorage,
            "enableRawLog": enableRawLog,
            "topicFormat": topicFormat,
            "fileEncoding": fileEncoding,
            "maxDepth": maxDepth,
            "preserve": preserve,
            "preserveDepth": preserveDepth,
            "filterKey": filterKey,
            "filterRegex": filterRegex
        }
        input_detail.update(extended_items)

        ConfigDetailBase.__init__(self, logstoreName, configName, "file", createTime=createTime, modifyTime=modifyTime,
                                  logSample=logSample, **input_detail)


class SimpleFileConfigDetail(ConfigDetailBase):
    """The logtail config for simple mode

    :type logstoreName: string
    :param logstoreName: the logstore name

    :type configName: string
    :param configName: the config name

    :type logPath: string
    :param logPath: folder of log path /apsara/nuwa/

    :type filePattern: string
    :param filePattern: file path, e.g. *.log, it will be /apsara/nuwa/.../*.log

    :type localStorage: bool
    :param localStorage: if use local cache 1GB when logtail is offline. default is True.

    :type enableRawLog: bool
    :param enableRawLog: if upload raw data in content, default is False

    :type topicFormat: string
    :param topicFormat: "none", "group_topic" or regex to extract value from file path e.g. "/test/(\w+).log" will extract each file as topic, default is "none"

    :type fileEncoding: string
    :param fileEncoding: "utf8" or "gbk" so far

    :type maxDepth: int
    :param maxDepth: max depth of folder to scan, by default its 100, 0 means just scan the root folder

    :type preserve: bool
    :param preserve: if preserve time-out, by default is False, 30 min time-out if set it as True

    :type preserveDepth: int
    :param preserveDepth: time-out folder depth. 1-3

    :type filterKey: string list
    :param filterKey: only keep log which match the keys. e.g. ["city", "location"] will only scan files math the two fields

    :type filterRegex: string list
    :param filterRegex: matched value for filterKey, e.g. ["shanghai|beijing|nanjing", "east"] note, it's regex value list

    :type createTime: int
    :param createTime: timestamp of created, only useful when getting data from REST

    :type modifyTime: int
    :param modifyTime: timestamp of last modified time, only useful when getting data from REST

    :type extended_items: dict
    :param extended_items: extended items

    """

    MANDATORY_FIELDS_DETAIL = ConfigDetailBase.MANDATORY_FIELDS_DETAIL \
                              + ["logPath", "filePattern"]
    DEFAULT_DETAIL_FIELDS = ConfigDetailBase.DEFAULT_DETAIL_FIELDS \
                            + [("logType", 'common_reg_log'), ("regex", '(.*)'), ("key", ['content']),
                               ("timeFormat", ''), ("topicFormat", "none")]

    def __init__(self, logstoreName, configName, logPath, filePattern, localStorage=None,
                 enableRawLog=None, topicFormat=None,
                 fileEncoding=None, maxDepth=None, preserve=None, preserveDepth=None, filterKey=None,
                 filterRegex=None, **extended_items):
        input_detail = {
            "logPath": logPath,
            "filePattern": filePattern,
            "localStorage": localStorage,
            "enableRawLog": enableRawLog,
            "topicFormat": topicFormat,
            "fileEncoding": fileEncoding,
            "maxDepth": maxDepth,
            "preserve": preserve,
            "preserveDepth": preserveDepth,
            "filterKey": filterKey,
            "filterRegex": filterRegex
        }
        input_detail.update(extended_items)

        ConfigDetailBase.__init__(self, logstoreName, configName, "file", **input_detail)


class FullRegFileConfigDetail(ConfigDetailBase):
    """The logtail config for full regex mode

    :type logstoreName: string
    :param logstoreName: the logstore name

    :type configName: string
    :param configName: the config name

    :type logPath: string
    :param logPath: folder of log path /apsara/nuwa/

    :type filePattern: string
    :param filePattern: file path, e.g. *.log, it will be /apsara/nuwa/.../*.log

    :type logSample: string
    :param logSample: log sample. e.g. shanghai|2000|east

    :type logBeginRegex: string
    :param logBeginRegex: regex to match line, None means '.*', just single line mode.

    :type regex: string
    :param regex: regex to extract fields form log. None means (.*), just capture whole line

    :type key: string list
    :param key: keys to map the fields like ["city", "population", "location"]. None means ["content"]

    :type timeFormat: string
    :param timeFormat: whe timeKey is not None, set its format, refer to https://help.aliyun.com/document_detail/28980.html?spm=5176.2020520112.113.4.2243b18eHkxdNB

    :type localStorage: bool
    :param localStorage: if use local cache 1GB when logtail is offline. default is True.

    :type enableRawLog: bool
    :param enableRawLog: if upload raw data in content, default is False

    :type topicFormat: string
    :param topicFormat: "none", "group_topic" or regex to extract value from file path e.g. "/test/(\w+).log" will extract each file as topic, default is "none"

    :type fileEncoding: string
    :param fileEncoding: "utf8" or "gbk" so far

    :type maxDepth: int
    :param maxDepth: max depth of folder to scan, by default its 100, 0 means just scan the root folder

    :type preserve: bool
    :param preserve: if preserve time-out, by default is False, 30 min time-out if set it as True

    :type preserveDepth: int
    :param preserveDepth: time-out folder depth. 1-3

    :type filterKey: string list
    :param filterKey: only keep log which match the keys. e.g. ["city", "location"] will only scan files math the two fields

    :type filterRegex: string list
    :param filterRegex: matched value for filterKey, e.g. ["shanghai|beijing|nanjing", "east"] note, it's regex value list

    :type createTime: int
    :param createTime: timestamp of created, only useful when getting data from REST

    :type modifyTime: int
    :param modifyTime: timestamp of last modified time, only useful when getting data from REST

    :type extended_items: dict
    :param extended_items: extended items

    """
    MANDATORY_FIELDS_DETAIL = ConfigDetailBase.MANDATORY_FIELDS_DETAIL \
                              + ["logPath", "filePattern"]
    DEFAULT_DETAIL_FIELDS = ConfigDetailBase.DEFAULT_DETAIL_FIELDS \
                            + [("logType", 'common_reg_log'), ("regex", '(.*)'),
                               ("key", ['content']), ("timeFormat", '')]

    def __init__(self, logstoreName, configName, logPath, filePattern, logSample,
                 logBeginRegex=None, regex=None, key=None, timeFormat=None,
                 localStorage=None,
                 enableRawLog=None, topicFormat=None,
                 fileEncoding=None, maxDepth=None, preserve=None, preserveDepth=None, filterKey=None,
                 filterRegex=None, **extended_items):
        input_detail = {
            "logPath": logPath,
            "filePattern": filePattern,
            "logBeginRegex": logBeginRegex,
            "regex": regex,
            "key": key,
            "timeFormat": timeFormat,
            "localStorage": localStorage,
            "enableRawLog": enableRawLog,
            "topicFormat": topicFormat,
            "fileEncoding": fileEncoding,
            "maxDepth": maxDepth,
            "preserve": preserve,
            "preserveDepth": preserveDepth,
            "filterKey": filterKey,
            "filterRegex": filterRegex
        }
        input_detail.update(extended_items)

        ConfigDetailBase.__init__(self, logstoreName, configName, "file", logSample=logSample, **input_detail)


class JsonFileConfigDetail(ConfigDetailBase):
    """The logtail config for json mode

    :type logstoreName: string
    :param logstoreName: the logstore name

    :type configName: string
    :param configName: the config name

    :type logPath: string
    :param logPath: folder of log path /apsara/nuwa/

    :type filePattern: string
    :param filePattern: file path, e.g. *.log, it will be /apsara/nuwa/.../*.log

    :type timeKey: string
    :param timeKey: one key name in `key` to set the time or set it None to use system time.

    :type timeFormat: string
    :param timeFormat: whe timeKey is not None, set its format, refer to https://help.aliyun.com/document_detail/28980.html?spm=5176.2020520112.113.4.2243b18eHkxdNB

    :type localStorage: bool
    :param localStorage: if use local cache 1GB when logtail is offline. default is True.

    :type enableRawLog: bool
    :param enableRawLog: if upload raw data in content, default is False

    :type topicFormat: string
    :param topicFormat: "none", "group_topic" or regex to extract value from file path e.g. "/test/(\w+).log" will extract each file as topic, default is "none"

    :type fileEncoding: string
    :param fileEncoding: "utf8" or "gbk" so far

    :type maxDepth: int
    :param maxDepth: max depth of folder to scan, by default its 100, 0 means just scan the root folder

    :type preserve: bool
    :param preserve: if preserve time-out, by default is False, 30 min time-out if set it as True

    :type preserveDepth: int
    :param preserveDepth: time-out folder depth. 1-3

    :type filterKey: string list
    :param filterKey: only keep log which match the keys. e.g. ["city", "location"] will only scan files math the two fields

    :type filterRegex: string list
    :param filterRegex: matched value for filterKey, e.g. ["shanghai|beijing|nanjing", "east"] note, it's regex value list

    :type createTime: int
    :param createTime: timestamp of created, only useful when getting data from REST

    :type modifyTime: int
    :param modifyTime: timestamp of last modified time, only useful when getting data from REST

    :type extended_items: dict
    :param extended_items: extended items

    """
    MANDATORY_FIELDS_DETAIL = ConfigDetailBase.MANDATORY_FIELDS_DETAIL \
                              + ["logPath", "filePattern"]
    DEFAULT_DETAIL_FIELDS = ConfigDetailBase.DEFAULT_DETAIL_FIELDS \
                            + [("logType", "json_log"), ('localStorage', True),
                               ("timeFormat", ''), ("topicFormat", "none")]

    def __init__(self, logstoreName, configName, logPath, filePattern, timeKey='', timeFormat=None,
                 localStorage=None, enableRawLog=None, topicFormat=None,
                 fileEncoding=None, maxDepth=None, preserve=None, preserveDepth=None, filterKey=None,
                 filterRegex=None, createTime=None, modifyTime=None, **extended_items):
        input_detail = {
            "logPath": logPath,
            "filePattern": filePattern,
            "timeFormat": timeFormat,
            "timeKey": timeKey,
            "localStorage": localStorage,
            "enableRawLog": enableRawLog,
            "topicFormat": topicFormat,
            "fileEncoding": fileEncoding,
            "maxDepth": maxDepth,
            "preserve": preserve,
            "preserveDepth": preserveDepth,
            "filterKey": filterKey,
            "filterRegex": filterRegex
        }
        input_detail.update(extended_items)

        ConfigDetailBase.__init__(self, logstoreName, configName, "file", createTime=createTime, modifyTime=modifyTime,
                                  **input_detail)


class ApsaraFileConfigDetail(ConfigDetailBase):
    """The logtail config for Apsara mode

    :type logstoreName: string
    :param logstoreName: the logstore name

    :type configName: string
    :param configName: the config name

    :type logPath: string
    :param logPath: folder of log path /apsara/nuwa/

    :type filePattern: string
    :param filePattern: file path, e.g. *.log, it will be /apsara/nuwa/.../*.log

    :type logBeginRegex: string
    :param logBeginRegex: regex to match line, None means '.*', just single line mode.

    :type localStorage: bool
    :param localStorage: if use local cache 1GB when logtail is offline. default is True.

    :type enableRawLog: bool
    :param enableRawLog: if upload raw data in content, default is False

    :type topicFormat: string
    :param topicFormat: "none", "group_topic" or regex to extract value from file path e.g. "/test/(\w+).log" will extract each file as topic, default is "none"

    :type fileEncoding: string
    :param fileEncoding: "utf8" or "gbk" so far

    :type maxDepth: int
    :param maxDepth: max depth of folder to scan, by default its 100, 0 means just scan the root folder

    :type preserve: bool
    :param preserve: if preserve time-out, by default is False, 30 min time-out if set it as True

    :type preserveDepth: int
    :param preserveDepth: time-out folder depth. 1-3

    :type filterKey: string list
    :param filterKey: only keep log which match the keys. e.g. ["city", "location"] will only scan files math the two fields

    :type filterRegex: string list
    :param filterRegex: matched value for filterKey, e.g. ["shanghai|beijing|nanjing", "east"] note, it's regex value list

    :type createTime: int
    :param createTime: timestamp of created, only useful when getting data from REST

    :type modifyTime: int
    :param modifyTime: timestamp of last modified time, only useful when getting data from REST

    :type extended_items: dict
    :param extended_items: extended items

    """
    MANDATORY_FIELDS_DETAIL = ConfigDetailBase.MANDATORY_FIELDS_DETAIL \
                              + ["logPath", "filePattern", "logBeginRegex"]
    DEFAULT_DETAIL_FIELDS = ConfigDetailBase.DEFAULT_DETAIL_FIELDS \
                            + [("logType", "apsara_log"), ("topicFormat", "none")]

    def __init__(self, logstoreName, configName, logPath, filePattern, logBeginRegex,
                 localStorage=None, enableRawLog=None, topicFormat=None,
                 fileEncoding=None, maxDepth=None, preserve=None, preserveDepth=None, filterKey=None,
                 filterRegex=None, createTime=None, modifyTime=None, **extended_items):
        input_detail = {
            "logPath": logPath,
            "filePattern": filePattern,
            "logBeginRegex": logBeginRegex,
            "localStorage": localStorage,
            "enableRawLog": enableRawLog,
            "topicFormat": topicFormat,
            "fileEncoding": fileEncoding,
            "maxDepth": maxDepth,
            "preserve": preserve,
            "preserveDepth": preserveDepth,
            "filterKey": filterKey,
            "filterRegex": filterRegex
        }
        input_detail.update(extended_items)

        ConfigDetailBase.__init__(self, logstoreName, configName, "file", createTime=createTime, modifyTime=modifyTime,
                                  **input_detail)


class SyslogConfigDetail(ConfigDetailBase):
    """The logtail config for syslog mode

    :type logstoreName: string
    :param logstoreName: the logstore name

    :type configName: string
    :param configName: the config name

    :type tag: string
    :param tag: tag for the log captured

    :type localStorage: bool
    :param localStorage: if use local cache 1GB when logtail is offline. default is True.

    :type createTime: int
    :param createTime: timestamp of created, only useful when getting data from REST

    :type modifyTime: int
    :param modifyTime: timestamp of last modified time, only useful when getting data from REST

    :type extended_items: dict
    :param extended_items: extended items

    """
    MANDATORY_FIELDS_DETAIL = ConfigDetailBase.MANDATORY_FIELDS_DETAIL + ["tag"]

    def __init__(self, logstoreName, configName, tag,
                 localStorage=None, createTime=None, modifyTime=None, **extended_items):
        input_detail = {
            "tag": tag,
            "localStorage": localStorage
        }
        input_detail.update(extended_items)

        ConfigDetailBase.__init__(self, logstoreName, configName, "syslog", createTime=createTime,
                                  modifyTime=modifyTime,
                                  **input_detail)


class LogtailConfigGenerator(object):
    """
    Generator of Logtial config
    """

    @staticmethod
    def generate_simple_log_config(json_value):
        return SimpleFileConfigDetail.from_json(json_value)

    @staticmethod
    def generate_json_config(json_value):
        return JsonFileConfigDetail.from_json(json_value)

    @staticmethod
    def generate_syslog_config(json_value):
        return SyslogConfigDetail.from_json(json_value)

    @staticmethod
    def generate_separator_config(json_value):
        return SeperatorFileConfigDetail.from_json(json_value)

    @staticmethod
    def generate_full_regex_config(json_value):
        return FullRegFileConfigDetail.from_json(json_value)

    @staticmethod
    def generate_apsara_config(json_value):
        return ApsaraFileConfigDetail.from_json(json_value)

    @staticmethod
    def generate_plugin_config(json_value):
        return PluginConfigDetail.from_json(json_value)

    @staticmethod
    def generate_config(json_value):
        input_type = json_value.get("inputType", "")
        if input_type == LogtailInputType.PLUGIN.value:
            return LogtailConfigGenerator.generate_plugin_config(json_value)
        elif input_type == LogtailInputType.SYSLOG.value:
            return LogtailConfigGenerator.generate_syslog_config(json_value)
        elif input_type == LogtailInputType.FILE.value:
            log_type = json_value["inputDetail"].get("logType", "")
            if log_type == LogtailType.JSON.value:
                return LogtailConfigGenerator.generate_json_config(json_value)
            elif log_type == LogtailType.FULL_REGEX.value:
                # use simple which is also full actually
                return LogtailConfigGenerator.generate_simple_log_config(json_value)
            elif log_type == LogtailType.SEPARATOR.value:
                return LogtailConfigGenerator.generate_separator_config(json_value)
            elif log_type == LogtailType.APSARA.value:
                return LogtailConfigGenerator.generate_apsara_config(json_value)

        raise LogException("InvalidInput", "input json string missed necessary fields: "
                                           "input_type and/or logType")

    @staticmethod
    def from_json(json_value):
        return LogtailConfigGenerator.generate_config(json_value)
