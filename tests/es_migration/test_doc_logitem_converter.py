#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.


import unittest

from aliyun.log import LogItem
from aliyun.log.es_migration.doc_logitem_converter import DocLogItemConverter


class TestDocLogItemConverter(unittest.TestCase):

    def setUp(self):
        self.doc = {
            "sort": [2],
            "_type": "_doc",
            "_source": {
                "es_long": 1,
                "es_short": -2,
                "es_geo_shape": {
                    "type": "point",
                    "coordinates": [
                        -77.03653,
                        38.897676]
                },
                "es_binary": "U29tZSBiaW5hcnkgYmxvYg==",
                "es_long_range": {"lt": 20},
                "es_float_range": {"lt": 4.56, "gt": 1.23},
                "es_date_range": {
                    "gte": "2015-10-31 12:00:00",
                    "lte": "2015-11-01"
                },
                "es_integer_range": {"gte": 10, "lte": 20},
                "es_byte": 127,
                "es_scaled_float": 12.3456789,
                "es_text": "es_text",
                "es_keyword": "es_keyword",
                "es_boolean": "true",
                "es_geo_point": {"lat": 41.12, "lon": -71.34},
                "es_double": 12.3456789,
                "es_date": "2018-07-23 14:00:00",
                "es_double_range": {"gte": 1.23},
                "es_float": -3.14,
                "es_object": {
                    "sub_long": 123,
                    "sub_boolean": "false",
                    "sub_date": 1420070400001,
                    "sub_byte": -128,
                    "sub_double": 12.345,
                    "sub_object": {
                        "sub_sub_text": "sub_sub_text",
                        "sub_sub_boolean": "true"
                    },
                    "sub_text": "sub_text",
                    "sub_double_range": {"lt": 4.56, "gt": 1.23}
                },
                "es_ip_range": "192.168.0.0/16",
                "es_half_float": 12.3456789,
                "es_array": ["x", "y", "z"]
            },
            "_score": None,
            "_index": "all_data_types",
            "_id": "1"
        }

    def test_to_log_item(self):
        log_item = DocLogItemConverter.to_log_item(self.doc, "es_date")
        self.assertEqual(1532354400, log_item.get_time())

    def test_get_index(self):
        index = DocLogItemConverter.get_index(self.doc)
        self.assertEqual("all_data_types", index)

    def test_add_index(self):
        log_item = LogItem()
        doc = {
            "_index": "index1"
        }
        DocLogItemConverter._add_index(doc, log_item)
        self.assertSetEqual({("__tag__:_index", "index1")}, set(log_item.contents))

    def test_add_index_without_index_field(self):
        log_item = LogItem()
        doc = {
            "_id": 1,
            "_source": {},
            "_type": "_doc"
        }
        DocLogItemConverter._add_index(doc, log_item)
        self.assertSetEqual(set(), set(log_item.contents))

    def test_add_type(self):
        log_item = LogItem()
        doc = {
            "_type": "_doc"
        }
        DocLogItemConverter._add_type(doc, log_item)
        self.assertSetEqual({("__tag__:_type", "_doc")}, set(log_item.contents))

    def test_add_type_without_index_field(self):
        log_item = LogItem()
        doc = {
            "_id": 1,
            "_source": {},
            "_index": "all_data_types"
        }
        DocLogItemConverter._add_type(doc, log_item)
        self.assertSetEqual(set(), set(log_item.contents))

    def test_add_id(self):
        log_item = LogItem()
        doc = {
            "_id": 1,
            "_source": {},
            "_index": "all_data_types",
            "_type": "_doc"
        }
        DocLogItemConverter._add_id(doc, log_item)
        self.assertSetEqual({("_id", "1")}, set(log_item.contents))

    def test_add_type_without_id(self):
        log_item = LogItem()
        doc = {
            "_source": {},
            "_index": "all_data_types",
            "_type": "_doc"
        }
        DocLogItemConverter._add_id(doc, log_item)
        self.assertSetEqual(set(), set(log_item.contents))


if __name__ == '__main__':
    unittest.main()
