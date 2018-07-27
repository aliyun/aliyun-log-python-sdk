#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.


from .. import IndexConfig, IndexKeyConfig, IndexLineConfig
from ..index_config import IndexJsonKeyConfig


class AliyunLogFieldType(object):
    TEXT = "text"
    LONG = "long"
    DOUBLE = "double"
    JSON = "json"


class DocRangeComparator(object):
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
        key_config_list = cls.to_key_config_list(properties=mapping["properties"])
        index_config = IndexConfig(line_config=line_config, key_config_list=key_config_list)
        return index_config

    @classmethod
    def to_key_config_list(cls, properties):
        key_config_list = {"_id": cls.handle_id()}
        if not properties:
            return key_config_list
        for field_name, field_desc in properties.items():
            if "type" in field_desc:
                field_type = field_desc["type"]
                if field_type not in field_type_handlers:
                    continue
                key_config = field_type_handlers[field_type]()
                key_config_list[field_name] = key_config
            elif "properties" in field_desc:
                key_config = cls.handle_properties(field_desc["properties"])
                key_config_list[field_name] = key_config
            else:
                raise Exception("invalid field_desc '%s'" % field_desc)
        return key_config_list

    @classmethod
    def handle_id(cls):
        return IndexKeyConfig(
            index_type=AliyunLogFieldType.TEXT,
            doc_value=True
        )

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

    @classmethod
    def handle_properties(cls, properties):
        json_key_config = IndexJsonKeyConfig()
        key_configs = cls.parse_properties(None, properties)
        for key_name, key_config in key_configs.items():
            json_key_config.add_key(key_name=key_name, key_type=key_config.index_type, doc_value=key_config.doc_value)
        return IndexKeyConfig(
            index_type=AliyunLogFieldType.JSON,
            json_key_config=json_key_config
        )

    @classmethod
    def parse_properties(cls, prefix, properties):
        key_config_list = {}
        for field_name, field_desc in properties.items():
            if prefix:
                field_name = prefix + "." + field_name
            if "type" in field_desc:
                field_type = field_desc["type"]
                if field_type not in field_type_handlers:
                    continue
                key_config = field_type_handlers[field_type]()
                key_config_list[field_name] = key_config
            elif "properties" in field_desc:
                sub_key_config_list = cls.parse_properties(field_name, field_desc["properties"])
                key_config_list.update(sub_key_config_list)
            else:
                raise Exception("invalid field_desc '%s'" % field_desc)
        return key_config_list


field_type_handlers = {
    "text": MappingIndexConverter.handle_text,
    "keyword": MappingIndexConverter.handle_keyword,
    "long": MappingIndexConverter.handle_long,
    "integer": MappingIndexConverter.handle_integer,
    "short": MappingIndexConverter.handle_short,
    "byte": MappingIndexConverter.handle_byte,
    "double": MappingIndexConverter.handle_double,
    "float": MappingIndexConverter.handle_float,
    "half_float": MappingIndexConverter.handle_half_float,
    "scaled_float": MappingIndexConverter.handle_scaled_float,
    "date": MappingIndexConverter.handle_date,
    "boolean": MappingIndexConverter.handle_boolean,
    "integer_range": MappingIndexConverter.handle_integer_range,
    "float_range": MappingIndexConverter.handle_float_range,
    "long_range": MappingIndexConverter.handle_long_range,
    "double_range": MappingIndexConverter.handle_double_range,
    "date_range": MappingIndexConverter.handle_date_range,
    "ip_range": MappingIndexConverter.handle_ip_range,
    "geo_point": MappingIndexConverter.handle_geo_point,
    "geo_shape": MappingIndexConverter.handle_geo_shape,
    "ip": MappingIndexConverter.handle_ip,
}
