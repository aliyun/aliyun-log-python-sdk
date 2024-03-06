#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

import time


class IndexJsonKeyConfig(object):
    """ The index config of a special json type key

    :type index_all: bool
    :param index_all: True if all string value in the json key should be indexed

    :type max_depth: int
    :param max_depth: if index_all is true, only if the json value depth <= max_depth will be index

    :type alias : string
    :param alias : alias name for index key
    """

    def __init__(self, index_all=True, max_depth=-1, alias=None):
        self.index_all = index_all
        self.max_depth = max_depth
        self.alias = alias
        self.json_keys = {}

    """
    Inner key config in json, if the json value is :
    json_key : {
        "map_1" : {
            "k_1" : "v_1",
            "k_2" : 100
        },
        "k_3" : 200.0
    }

    :type key_name : string
    :param key_name : key name , e.g  "map_1.k_1",  "k_3"

    :type index_type: string
    :param index_type: one of ['text', 'long', 'double']

    :type doc_value : bool
    :param doc_value : if save doc value

    :type alias : string
    :param alias : alias name for index key
    """

    def add_key(self, key_name, key_type, doc_value=False, alias=None):
        if key_type != 'text' and key_type != 'long' and key_type != 'double':
            return
        self.json_keys[key_name] = {}
        self.json_keys[key_name]["type"] = key_type
        self.json_keys[key_name]["doc_value"] = doc_value
        if alias is not None:
            self.json_keys[key_name]["alias"] = alias

    def to_json(self, json_value):
        json_value["index_all"] = self.index_all
        json_value["max_depth"] = self.max_depth
        json_value["json_keys"] = self.json_keys

    def from_json(self, json_value):
        self.index_all = json_value.get("index_all", True)
        self.max_depth = json_value.get("max_depth", -1)
        self.alias = None
        self.json_keys = {}
        if "alias" in json_value:
            self.alias = json_value["alias"]
        if "json_keys" in json_value:
            self.json_keys = json_value["json_keys"]


class IndexKeyConfig(object):
    """ The index config of a special log key

    :type token_list: string list
    :param token_list: the token config list, e.g ["," , "\t" , "\n" , " " , ";"]

    :type case_sensitive: bool
    :param case_sensitive: True if the value in the log keys is case sensitive, False other wise

    :type index_type: string
    :param index_type: one of ['text', 'long', 'double', 'json']

    :type doc_value: bool
    :param doc_value: True if enable doc_value, used for fast sql execution

    :type alias : string
    :param alias : alias name for index key

    :type json_key_config : IndexJsonKeyConfig
    :param json_key_config : configs for "json" type

    :type chinese: bool
    :param chinese: enable Chinese words segmentation

    """

    def __init__(self, token_list=None, case_sensitive=False, index_type='text', doc_value=False, alias=None,
                 json_key_config=None, chinese=None):
        if token_list is None:
            token_list = []
        self.token_list = token_list
        self.case_sensitive = case_sensitive
        self.index_type = index_type
        self.doc_value = doc_value
        self.alias = alias
        self.json_key_config = json_key_config
        self.chn = chinese

    def set_json_key_config(self, json_key_config):
        self.json_key_config = json_key_config

    def get_json_key_config(self):
        return self.json_key_config

    def to_json(self):
        json_value = {}
        if self.index_type != "" and self.index_type is not None:
            json_value['type'] = self.index_type
        if self.index_type == 'text' or self.index_type == 'json':
            json_value["token"] = self.token_list
            json_value["caseSensitive"] = bool(self.case_sensitive)
        if self.alias is not None:
            json_value['alias'] = self.alias
        json_value["doc_value"] = bool(self.doc_value)

        if self.chn is not None:
            json_value['chn'] = self.chn

        if self.index_type == "json":
            self.json_key_config.to_json(json_value)

        return json_value

    def from_json(self, json_value):
        self.index_type = 'text'
        if 'type' in json_value:
            self.index_type = json_value['type']
        if self.index_type in ('text', 'json'):
            self.token_list = json_value["token"]
            self.chn = None
            if "chn" in json_value:
                self.chn = bool(json_value["chn"])
        self.case_sensitive = bool(json_value.get("caseSensitive", False))
        if 'doc_value' in json_value:
            self.doc_value = bool(json_value["doc_value"])
        if 'alias' in json_value:
            self.alias = json_value['alias']
        if self.index_type == 'json':
            self.json_key_config = IndexJsonKeyConfig()
            self.json_key_config.from_json(json_value)


class IndexLineConfig(object):
    """ The index config of the log line

    :type token_list: string list
    :param token_list: the token config list, e.g ["," , "\t" , "\n" , " " , ";"]

    :type case_sensitive: bool
    :param case_sensitive: True if the value in the log keys is case sensitive, False other wise

    :type include_keys: string list
    :param include_keys: deprecated, will be removed in future version.

    :type exclude_keys: string list
    :param exclude_keys: deprecated, will be removed in future version.

    :type chinese: bool
    :param chinese: enable Chinese words segmentation

    """

    def __init__(self, token_list=None, case_sensitive=False, include_keys=None, exclude_keys=None, chinese=None):
        if token_list is None:
            token_list = []
        self.token_list = token_list
        self.case_sensitive = case_sensitive
        self.chn = bool(chinese)

    def to_json(self):
        json_value = {"token": self.token_list, "caseSensitive": bool(self.case_sensitive)}
        if self.chn is not None:
            json_value["chn"] = bool(self.chn)

        return json_value

    def from_json(self, json_value):
        self.token_list = json_value["token"]
        self.case_sensitive = bool(json_value.get("caseSensitive", False))
        self.chn = bool(json_value["chn"]) if "chn" in json_value else None


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

    :type log_reduce: bool
    :param log_reduce: if to enable logreduce

    """

    def __init__(self, ttl=1, line_config=None, key_config_list=None, all_keys_config=None, log_reduce=None):
        if key_config_list is None:
            key_config_list = {}
        self.ttl = ttl
        self.all_keys_config = all_keys_config
        self.line_config = line_config
        self.key_config_list = key_config_list
        self.modify_time = int(time.time())
        self.log_reduce = log_reduce
        self.log_reduce_black_list = None
        self.log_reduce_white_list = None
        self.docvalue_max_text_len = 0

    '''
    :type max_len : int
    :param max_len : the max len  of the docvalue
    '''

    def set_docvalue_max_text_len(self, max_len):
        self.docvalue_max_text_len = max_len

    '''
    :type black_list : list
    :param black_list : the list of black list keys for log reduce, e.g black_list = ["key_1", "key_2"]
    '''

    def set_log_reduce_black_list(self, black_list):
        self.log_reduce_black_list = black_list

    '''
    :type white_list : list
    :param white_list : the list of white list keys for log reduce, e.g white_list = ["key_1", "key_2"]
    '''

    def set_log_reduce_white_list(self, white_list):
        self.log_reduce_white_list = white_list

    def to_json(self):
        json_value = {}
        if self.line_config is not None:
            json_value["line"] = self.line_config.to_json()
        if len(self.key_config_list) != 0:
            json_value["keys"] = dict((key, value.to_json()) for key, value in self.key_config_list.items())

        if self.all_keys_config is not None:
            json_value["all_keys"] = self.all_keys_config.to_json()

        if self.log_reduce is not None:
            json_value["log_reduce"] = self.log_reduce

        if self.docvalue_max_text_len > 0:
            json_value["max_text_len"] = self.docvalue_max_text_len

        if self.log_reduce_white_list != None:
            json_value["log_reduce_white_list"] = self.log_reduce_white_list

        elif self.log_reduce_black_list != None:
            json_value["log_reduce_black_list"] = self.log_reduce_black_list

        return json_value

    def from_json(self, json_value):
        self.ttl = json_value.get("ttl", 0)
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
        self.log_reduce = json_value.get('log_reduce', None)
        self.log_reduce_white_list = json_value.get('log_reduce_white_list', None)
        self.log_reduce_black_list = json_value.get('log_reduce_black_list', None)

        self.docvalue_max_text_len = json_value.get('max_text_len', 0)

        self.modify_time = json_value.get("lastModifyTime", int(time.time()))
