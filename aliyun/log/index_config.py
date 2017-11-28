#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

import time

from .util import Util


class IndexKeyConfig(object):
    """ The index config of a special log key

    :type token_list: string list
    :param token_list: the token config list, e.g ["," , "\t" , "\n" , " " , ";"] 

    :type case_sensitive: bool
    :param case_sensitive: True if the value in the log keys is case sensitive, False other wise 

    :type index_type: string
    :param index_type: one of ['text', 'long', 'double']

    :type doc_value: bool
    :param doc_value: True if enable doc_value, used for fast sql execution

    :type alias : string
    :param alias : alias name for index key
    """

    def __init__(self, token_list=None, case_sensitive=False, index_type='text', doc_value=False,alias=None):
        if token_list is None:
            token_list = []
        self.token_list = token_list
        self.case_sensitive = case_sensitive
        self.index_type = index_type
        self.doc_value = doc_value
        self.alias = alias

    def to_json(self):
        json_value = {}
        if self.index_type != "" and self.index_type is not None:
            json_value['type'] = self.index_type
        if self.index_type == 'text':
            json_value["token"] = self.token_list
            json_value["caseSensitive"] = bool(self.case_sensitive)
        if self.alias is not None:
            json_value['alias'] = self.alias;
        json_value["doc_value"] = bool(self.doc_value)
        return json_value

    def from_json(self, json_value):
        self.index_type = 'text'
        if 'type' in json_value:
            self.index_type = json_value['type']
        if self.index_type == 'text':
            self.token_list = json_value["token"]
            self.case_sensitive = bool(json_value["caseSensitive"])
        if 'doc_value' in json_value:
            self.doc_value = bool(json_value["doc_value"])
        if 'alias'  in json_value:
            self.alias = json_value['alias']


class IndexLineConfig(object):
    """ The index config of the log line

    :type token_list: string list
    :param token_list: the token config list, e.g ["," , "\t" , "\n" , " " , ";"] 

    :type case_sensitive: bool
    :param case_sensitive: True if the value in the log keys is case sensitive, False other wise 

    :type include_keys: string list
    :param include_keys: only the keys in include_keys should to be indexed, only one of include_keys and exclude_keys
     could exist at the same time,
                if bothe include_keys and exclude_keys are empty, then the full line will be indexed

    :type exclude_keys: string list
    :param exclude_keys: the keys in the exclude_keys list will not be indexed, others keys will be indexed 
    """

    def __init__(self, token_list=None, case_sensitive=False, include_keys=None, exclude_keys=None):
        if token_list is None:
            token_list = []
        self.token_list = token_list
        self.case_sensitive = case_sensitive
        self.include_keys = include_keys
        self.exclude_keys = exclude_keys

    def to_json(self):
        json_value = {"token": self.token_list, "caseSensitive": bool(self.case_sensitive)}

        if self.include_keys is not None:
            json_value["include_keys"] = self.include_keys
        if self.exclude_keys is not None:
            json_value["exclude_keys"] = self.exclude_keys
        return json_value

    def from_json(self, json_value):
        self.token_list = json_value["token"]
        self.case_sensitive = bool(json_value.get("caseSensitive", False))
        self.include_keys = json_value.get("include_keys", None)
        self.exclude_keys = json_value.get("exclude_keys", None)


class IndexConfig(object):
    """The index config of a logstore

    :type ttl: int
    :param ttl: this parameter is deprecated, the ttl is same as logstore's ttl

    :type line_config: IndexLineConfig
    :param line_config: the index config of the whole log line

    :type key_config_list: dict
    :param key_config_list: dict (string => IndexKeyConfig), the index key configs of the keys

    :type all_keys_config: IndexKeyConfig
    :param all_keys_config: the key config of all keys, the new create logstore should never user this param, it only used to compatible with old config
    """

    def __init__(self, ttl=1, line_config=None, key_config_list=None, all_keys_config=None):
        if key_config_list is None:
            key_config_list = {}
        self.ttl = ttl
        self.all_keys_config = all_keys_config
        self.line_config = line_config
        self.key_config_list = key_config_list
        self.modify_time = int(time.time())

    def to_json(self):
        json_value = {"ttl": self.ttl}
        if self.line_config is not None:
            json_value["line"] = self.line_config.to_json()
        if len(self.key_config_list) != 0:
            json_value["keys"] = dict((key, value.to_json()) for key, value in self.key_config_list.items())
            # for key, value in self.key_config_list.items():
            #     json_value["keys"][key] = value.to_json()

        if self.all_keys_config is not None:
            json_value["all_keys"] = self.all_keys_config.to_json()
        return json_value

    def from_json(self, json_value):
        self.ttl = json_value["ttl"]
        if "all_keys" in json_value:
            self.all_keys_config = IndexKeyConfig()
            self.all_keys_config.from_json(json_value["all_keys"])
        if "line" in json_value:
            self.line_config = IndexLineConfig()
            self.line_config.from_json(json_value["line"])
        if "keys" in json_value:
            self.key_config_list = {}
            key_configs = json_value["keys"]
            for key, value in key_configs.items():
                key_config = IndexKeyConfig()
                key_config.from_json(value)
                self.key_config_list[key] = key_config

        self.modify_time = json_value.get("lastModifyTime", int(time.time()))
