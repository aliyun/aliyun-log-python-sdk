#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.


import unittest

from aliyun.log.es_migration.mapping_index_converter import MappingIndexConverter


class TestMappingIndexConverter(unittest.TestCase):

    def test_to_index_config(self):
        mapping = {
            "properties": {
                "es_text": {
                    "type": "text"
                },
                "es_keyword": {
                    "type": "keyword"
                },
                "es_long": {
                    "type": "long"
                },
                "es_integer": {
                    "type": "integer"
                },
                "es_short": {
                    "type": "short"
                },
                "es_byte": {
                    "type": "byte"
                },
                "es_double": {
                    "type": "double"
                },
                "es_float": {
                    "type": "float"
                },
                "es_half_float": {
                    "type": "half_float"
                },
                "es_scaled_float": {
                    "type": "scaled_float",
                    "scaling_factor": 100
                },
                "es_date": {
                    "type": "date",
                    "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
                },
                "es_boolean": {
                    "type": "boolean"
                },
                "es_binary": {
                    "type": "binary"
                },
                "es_integer_range": {
                    "type": "integer_range"
                },
                "es_float_range": {
                    "type": "float_range"
                },
                "es_long_range": {
                    "type": "long_range"
                },
                "es_double_range": {
                    "type": "double_range"
                },
                "es_date_range": {
                    "type": "date_range",
                    "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
                },
                "es_ip_range": {
                    "type": "ip_range"
                },
                "es_object": {
                    "properties": {
                        "sub_text": {"type": "text"},
                        "sub_long": {"type": "long"},
                        "sub_double": {"type": "double"},
                        "sub_boolean": {"type": "boolean"},
                        "sub_date": {
                            "type": "date",
                            "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
                        },
                        "sub_byte": {"type": "byte"},
                        "sub_double_range": {
                            "type": "double_range"
                        },
                        "sub_object": {
                            "properties": {
                                "sub_text": {"type": "text"},
                                "sub_boolean": {"type": "boolean"}
                            }
                        }
                    }
                },
                "es_geo_point": {
                    "type": "geo_point"
                },
                "es_geo_shape": {
                    "type": "geo_shape"
                }
            }
        }
        index_config = MappingIndexConverter.to_index_config(mapping)
        line_config = index_config.line_config
        self.assertEqual(line_config.token_list, MappingIndexConverter.DEFAULT_TOKEN_LIST)
        self.assertTrue(line_config.chn)
        for field_name, key_config in index_config.key_config_list.iteritems():
            print field_name, key_config

    def test_to_index_config_with_none(self):
        index_config = MappingIndexConverter.to_index_config(None)
        self.assertEqual(index_config, None)


if __name__ == '__main__':
    unittest.main()
