#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.


import unittest

from aliyun.log.es_migration.index_logstore_mappings import \
    IndexLogstoreMappings


class TestIndexLogstoreMappings(unittest.TestCase):

    def setUp(self):
        logstore_index_mappings = '''
                    {
                        "logstore1": "my_index*", 
                        "logstore2": "a_index,b_index,c_index", 
                        "logstore3": "index1",
                        "logstore4": "xxx_*_yyy",
                        "logstore5": "not_exists"
                    }
                    '''
        self.index_lst = ["my_index", "my_index123", "a_index", "b_index", "d_index", "index1", "index2", "xxx__yyy",
                          "xxx_123_yyy", "xxxyyy"]
        self.mappings = IndexLogstoreMappings(index_lst=self.index_lst, logstore_index_mappings=logstore_index_mappings)

    def test_get_logstore(self):
        self.assertEqual("logstore1", self.mappings.get_logstore("my_index"))
        self.assertEqual("logstore1", self.mappings.get_logstore("my_index123"))
        self.assertIsNone(self.mappings.get_logstore("my_indexabcd"))
        self.assertIsNone(self.mappings.get_logstore("my_inde"))

        self.assertEqual("logstore2", self.mappings.get_logstore("a_index"), "logstore2")
        self.assertEqual("logstore2", self.mappings.get_logstore("b_index"))
        self.assertIsNone(self.mappings.get_logstore("c_index"))
        self.assertEqual("d_index", self.mappings.get_logstore("d_index"))

        self.assertEqual("logstore3", self.mappings.get_logstore("index1"))
        self.assertEqual("index2", self.mappings.get_logstore("index2"))

        self.assertEqual("logstore4", self.mappings.get_logstore("xxx__yyy"))
        self.assertEqual("logstore4", self.mappings.get_logstore("xxx_123_yyy"))
        self.assertIsNone(self.mappings.get_logstore("xxx_456_yyy"))
        self.assertEqual("xxxyyy", self.mappings.get_logstore("xxxyyy"))

    def test_get_indexes(self):
        self.assertListEqual(["my_index123", "my_index"], self.mappings.get_indexes("logstore1"))
        self.assertListEqual(["a_index", "b_index"], self.mappings.get_indexes("logstore2"))
        self.assertListEqual(["index1"], self.mappings.get_indexes("logstore3"))
        self.assertListEqual(["xxx_123_yyy", "xxx__yyy"], self.mappings.get_indexes("logstore4"))
        self.assertListEqual([], self.mappings.get_indexes("logstore5"))
        self.assertListEqual([], self.mappings.get_indexes("logstore6"))
        self.assertListEqual(["d_index"], self.mappings.get_indexes("d_index"))
        self.assertListEqual(["index2"], self.mappings.get_indexes("index2"))
        self.assertListEqual(["xxxyyy"], self.mappings.get_indexes("xxxyyy"))

    def test_get_all_logstores(self):
        expected_logstores = ["logstore1", "logstore2", "logstore3", "logstore4", "d_index", "index2", "xxxyyy"]
        self.assertSetEqual(set(expected_logstores), set(self.mappings.get_all_logstores()))

    def test_get_all_indexes(self):
        self.assertSetEqual(set(self.index_lst), set(self.mappings.get_all_indexes()))

    def test_get_match_index_lst(self):
        actual_match_index_lst = IndexLogstoreMappings._get_match_indexes("index", ["my_index", "your_index"])
        self.assertListEqual([], actual_match_index_lst)

        actual_match_index_lst = IndexLogstoreMappings._get_match_indexes(None, ["my_index", "your_index"])
        self.assertListEqual([], actual_match_index_lst)

        actual_match_index_lst = IndexLogstoreMappings._get_match_indexes("index", None)
        self.assertListEqual([], actual_match_index_lst)

        actual_match_index_lst = IndexLogstoreMappings._get_match_indexes(None, None)
        self.assertListEqual([], actual_match_index_lst)

        actual_match_index_lst = IndexLogstoreMappings._get_match_indexes("index",
                                                                          ["my_index", "index", "your_index"])
        self.assertListEqual(["index"], actual_match_index_lst)

        actual_match_index_lst = IndexLogstoreMappings._get_match_indexes("index*",
                                                                          ["index", "index123", "inde"])
        self.assertListEqual(["index", "index123"], actual_match_index_lst)

    def test_invalid_logstore_index_mappings(self):
        logstore_index_mappings = '''
                            {
                                "logstore1": "my_index*", 
                                "logstore2": "my_index"
                            }
                            '''
        index_lst = ["my_index", "my_index1", "my_index2"]
        with self.assertRaises(Exception):
            IndexLogstoreMappings(index_lst=index_lst, logstore_index_mappings=logstore_index_mappings)

    def test_get_logstore_empty_logstore_index_mappings(self):
        index_lst = ["index1", "index2"]
        mappings = IndexLogstoreMappings(index_lst=index_lst, logstore_index_mappings=None)
        self.assertSetEqual(set(index_lst), set(mappings.get_all_indexes()))
        self.assertSetEqual(set(index_lst), set(mappings.get_all_logstores()))
        self.assertListEqual(["index1"],mappings.get_indexes("index1"))
        self.assertListEqual(["index2"], mappings.get_indexes("index2"))
        self.assertEqual("index1", mappings.get_logstore("index1"))
        self.assertEqual("index2", mappings.get_logstore("index2"))


if __name__ == '__main__':
    unittest.main()
