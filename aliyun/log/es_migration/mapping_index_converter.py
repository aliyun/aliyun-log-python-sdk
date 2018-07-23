#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.


from enum import Enum

from aliyun.log import IndexConfig
from aliyun.log import IndexLineConfig
from aliyun.log import IndexKeyConfig
from aliyun.log.index_config import IndexJsonKeyConfig


class AliyunLogFieldType(Enum):
    TEXT = "text"
    LONG = "long"
    DOUBLE = "double"
    JSON = "json"


class DocRangeComparator(Enum):
    GT = "gt"
    LT = "lt"
    GTE = "gte"
    LTE = "lte"


class MappingIndexConverter(object):
    DEFAULT_TOKEN_LIST = [",", " ", "'", "\"", ";", "=", "(", ")", "[", "]", "{", "}", "?", "@", "&", "<", ">", "/",
                          ":", "\\n", "\\t", "\\r"]

    @classmethod
    def to_index_config(cls, mapping):
        if not mapping:
            return None
        line_config = IndexLineConfig(token_list=cls.DEFAULT_TOKEN_LIST, chinese=True)
        key_config_list = cls.to_key_config_list(prefix=None, properties=mapping["properties"])
        index_config = IndexConfig(line_config=line_config, key_config_list=key_config_list)
        return index_config

    @classmethod
    def to_key_config_list(cls, prefix, properties):
        if not properties:
            return {}
        key_config_list = {}
        for field_name, field_desc in properties.iteritems():
            if "type" in field_desc:
                field_type = field_desc["type"]
                if field_type not in field_type_handlers:
                    continue
                key_config = field_type_handlers[field_type]()
                if prefix:
                    field_name = prefix + "." + field_name
                key_config_list[field_name] = key_config
            elif "properties" in field_desc:
                sub_key_config_list = cls.to_key_config_list(field_name, field_desc["properties"])
                key_config_list.update(sub_key_config_list)
            else:
                raise Exception("invalid field_desc '%s'" % field_desc)
        return key_config_list

    @classmethod
    def handle_text(cls):
        return IndexKeyConfig(
            token_list=cls.DEFAULT_TOKEN_LIST,
            index_type=AliyunLogFieldType.TEXT,
            doc_value=True,
            chinese=True,
        )

    @classmethod
    def handle_keyword(cls):
        return IndexKeyConfig(
            index_type=AliyunLogFieldType.TEXT,
            doc_value=True,
            chinese=True,
        )

    @classmethod
    def handle_long(cls):
        return IndexKeyConfig(
            index_type=AliyunLogFieldType.LONG,
            doc_value=True,
        )

    @classmethod
    def handle_integer(cls):
        return IndexKeyConfig(
            index_type=AliyunLogFieldType.LONG,
            doc_value=True,
        )

    @classmethod
    def handle_short(cls):
        return IndexKeyConfig(
            index_type=AliyunLogFieldType.LONG,
            doc_value=True,
        )

    @classmethod
    def handle_byte(cls):
        return IndexKeyConfig(
            index_type=AliyunLogFieldType.LONG,
            doc_value=True,
        )

    @classmethod
    def handle_double(cls):
        return IndexKeyConfig(
            index_type=AliyunLogFieldType.DOUBLE,
            doc_value=True,
        )

    @classmethod
    def handle_float(cls):
        return IndexKeyConfig(
            index_type=AliyunLogFieldType.DOUBLE,
            doc_value=True,
        )

    @classmethod
    def handle_half_float(cls):
        return IndexKeyConfig(
            index_type=AliyunLogFieldType.DOUBLE,
            doc_value=True,
        )

    @classmethod
    def handle_scaled_float(cls):
        return IndexKeyConfig(
            index_type=AliyunLogFieldType.DOUBLE,
            doc_value=True,
        )

    @classmethod
    def handle_date(cls):
        return IndexKeyConfig(
            token_list=cls.DEFAULT_TOKEN_LIST,
            index_type=AliyunLogFieldType.TEXT,
            doc_value=True,
        )

    @classmethod
    def handle_boolean(cls):
        return IndexKeyConfig(
            index_type=AliyunLogFieldType.TEXT,
            doc_value=True,
        )

    @classmethod
    def handle_integer_range(cls):
        json_key_config = IndexJsonKeyConfig()
        json_key_config.add_key(key_name=DocRangeComparator.GT, key_type=AliyunLogFieldType.LONG, doc_value=True)
        json_key_config.add_key(key_name=DocRangeComparator.LT, key_type=AliyunLogFieldType.LONG, doc_value=True)
        json_key_config.add_key(key_name=DocRangeComparator.GTE, key_type=AliyunLogFieldType.LONG, doc_value=True)
        json_key_config.add_key(key_name=DocRangeComparator.LTE, key_type=AliyunLogFieldType.LONG, doc_value=True)
        return IndexKeyConfig(
            index_type=AliyunLogFieldType.JSON,
            json_key_config=json_key_config
        )

    @classmethod
    def handle_float_range(cls):
        json_key_config = IndexJsonKeyConfig()
        json_key_config.add_key(key_name=DocRangeComparator.GT, key_type=AliyunLogFieldType.DOUBLE, doc_value=True)
        json_key_config.add_key(key_name=DocRangeComparator.LT, key_type=AliyunLogFieldType.DOUBLE, doc_value=True)
        json_key_config.add_key(key_name=DocRangeComparator.GTE, key_type=AliyunLogFieldType.DOUBLE, doc_value=True)
        json_key_config.add_key(key_name=DocRangeComparator.LTE, key_type=AliyunLogFieldType.DOUBLE, doc_value=True)
        return IndexKeyConfig(
            index_type=AliyunLogFieldType.JSON,
            json_key_config=json_key_config
        )

    @classmethod
    def handle_long_range(cls):
        json_key_config = IndexJsonKeyConfig()
        json_key_config.add_key(key_name=DocRangeComparator.GT, key_type=AliyunLogFieldType.LONG, doc_value=True)
        json_key_config.add_key(key_name=DocRangeComparator.LT, key_type=AliyunLogFieldType.LONG, doc_value=True)
        json_key_config.add_key(key_name=DocRangeComparator.GTE, key_type=AliyunLogFieldType.LONG, doc_value=True)
        json_key_config.add_key(key_name=DocRangeComparator.LTE, key_type=AliyunLogFieldType.LONG, doc_value=True)
        return IndexKeyConfig(
            index_type=AliyunLogFieldType.JSON,
            json_key_config=json_key_config
        )

    @classmethod
    def handle_double_range(cls):
        json_key_config = IndexJsonKeyConfig()
        json_key_config.add_key(key_name=DocRangeComparator.GT, key_type=AliyunLogFieldType.DOUBLE, doc_value=True)
        json_key_config.add_key(key_name=DocRangeComparator.LT, key_type=AliyunLogFieldType.DOUBLE, doc_value=True)
        json_key_config.add_key(key_name=DocRangeComparator.GTE, key_type=AliyunLogFieldType.DOUBLE, doc_value=True)
        json_key_config.add_key(key_name=DocRangeComparator.LTE, key_type=AliyunLogFieldType.DOUBLE, doc_value=True)
        return IndexKeyConfig(
            index_type=AliyunLogFieldType.JSON,
            json_key_config=json_key_config
        )

    @classmethod
    def handle_date_range(cls):
        json_key_config = IndexJsonKeyConfig()
        json_key_config.add_key(key_name=DocRangeComparator.GT, key_type=AliyunLogFieldType.TEXT, doc_value=True)
        json_key_config.add_key(key_name=DocRangeComparator.LT, key_type=AliyunLogFieldType.TEXT, doc_value=True)
        json_key_config.add_key(key_name=DocRangeComparator.GTE, key_type=AliyunLogFieldType.TEXT, doc_value=True)
        json_key_config.add_key(key_name=DocRangeComparator.LTE, key_type=AliyunLogFieldType.TEXT, doc_value=True)
        return IndexKeyConfig(
            index_type=AliyunLogFieldType.JSON,
            json_key_config=json_key_config
        )

    @classmethod
    def handle_ip_range(cls):
        return IndexKeyConfig(
            index_type=AliyunLogFieldType.TEXT,
            doc_value=True,
        )

    @classmethod
    def handle_geo_point(cls):
        return IndexKeyConfig(
            token_list=cls.DEFAULT_TOKEN_LIST,
            index_type=AliyunLogFieldType.TEXT,
            doc_value=True,
        )

    @classmethod
    def handle_geo_shape(cls):
        return IndexKeyConfig(
            token_list=cls.DEFAULT_TOKEN_LIST,
            index_type=AliyunLogFieldType.TEXT,
            doc_value=True,
        )

    @classmethod
    def handle_ip(cls):
        return IndexKeyConfig(
            index_type=AliyunLogFieldType.TEXT,
            doc_value=True,
        )


field_type_handlers = {
    "text": MappingIndexConverter.handle_text,
    "keyword": MappingIndexConverter.handle_keyword,
    "long": MappingIndexConverter.handle_keyword,
    "integer": MappingIndexConverter.handle_keyword,
    "short": MappingIndexConverter.handle_keyword,
    "byte": MappingIndexConverter.handle_keyword,
    "double": MappingIndexConverter.handle_keyword,
    "float": MappingIndexConverter.handle_keyword,
    "half_float": MappingIndexConverter.handle_keyword,
    "scaled_float": MappingIndexConverter.handle_keyword,
    "date": MappingIndexConverter.handle_keyword,
    "boolean": MappingIndexConverter.handle_keyword,
    "integer_range": MappingIndexConverter.handle_keyword,
    "float_range": MappingIndexConverter.handle_keyword,
    "long_range": MappingIndexConverter.handle_keyword,
    "double_range": MappingIndexConverter.handle_keyword,
    "date_range": MappingIndexConverter.handle_keyword,
    "ip_range": MappingIndexConverter.handle_keyword,
    "geo_point": MappingIndexConverter.handle_geo_point,
    "geo_shape": MappingIndexConverter.handle_geo_shape,
    "ip": MappingIndexConverter.handle_ip,
}
