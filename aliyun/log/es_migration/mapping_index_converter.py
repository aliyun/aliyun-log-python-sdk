#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.


import time

from aliyun.log import IndexConfig
from aliyun.log import IndexLineConfig
from aliyun.log import IndexKeyConfig


class MappingIndexConverter(object):
    DEFAULT_TOKEN_LIST = [",", " ", "'", "\"", ";", "=", "(", ")", "[", "]", "{", "}", "?", "@", "&", "<", ">", "/",
                          ":", "\\n", "\\t", "\\r"]

    @classmethod
    def to_index_config(cls, mapping):
        if not mapping:
            return None
        line_config = IndexLineConfig(token_list=cls.DEFAULT_TOKEN_LIST, chinese=True)
        key_config_list = cls.to_key_config_list(mapping["properties"])
        index_config = IndexConfig(line_config=line_config, key_config_list=key_config_list)
        return index_config

    @classmethod
    def to_key_config_list(cls, properties):
        if not properties:
            return {}
        key_config_list = {}
        for field_name, field_desc in properties.iteritems():
            if "type" in field_desc:
                field_type = field_desc["type"]
            elif "properties" in field_desc:
                properties = field_desc["properties"]
            else:
                raise Exception("invalid field_desc '%s'" % field_desc)
            key_config = IndexKeyConfig()
        return key_config_list
