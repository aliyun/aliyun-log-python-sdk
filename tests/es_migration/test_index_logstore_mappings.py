#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.


import unittest

from aliyun.log.es_migration.index_logstore_mappings import IndexLogstoreMappings


class TestIndexLogstoreMappings(unittest.TestCase):

    def test_get_logstore(self):
        logstore_index_mappings = '''
            {
                "logstore1": "my_index*", 
                "logstore2": "a_index,b_index,c_index", 
                "logstore3": "index1",
                "logstore4": "xxx_*_yyy"
            }
            '''
        index_lst = ["my_index", "my_index123", "a_index", "b_index", "d_index", "index1", "index2", "xxx__yyy",
                     "xxx_123_yyy", "xxxyyy"]
        mapper = IndexLogstoreMappings(index_lst=index_lst, logstore_index_mappings=logstore_index_mappings)

        self.assertEqual(mapper.get_logstore("my_index"), "logstore1")
        self.assertEqual(mapper.get_logstore("my_index123"), "logstore1")
        self.assertIsNone(mapper.get_logstore("my_indexabcd"))
        self.assertIsNone(mapper.get_logstore("my_inde"))

        self.assertEqual(mapper.get_logstore("a_index"), "logstore2")
        self.assertEqual(mapper.get_logstore("b_index"), "logstore2")
        self.assertIsNone(mapper.get_logstore("c_index"))
        self.assertEqual(mapper.get_logstore("d_index"), "d_index")

        self.assertEqual(mapper.get_logstore("index1"), "logstore3")
        self.assertEqual(mapper.get_logstore("index2"), "index2")

        self.assertEqual(mapper.get_logstore("xxx__yyy"), "logstore4")
        self.assertEqual(mapper.get_logstore("xxx_123_yyy"), "logstore4")
        self.assertIsNone(mapper.get_logstore("xxx_456_yyy"))
        self.assertEqual(mapper.get_logstore("xxxyyy"), "xxxyyy")

    def test_get_match_index_lst(self):
        actual_match_index_lst = IndexLogstoreMappings._get_match_index_lst("index", ["my_index", "your_index"])
        self.assertListEqual(actual_match_index_lst, [])

        actual_match_index_lst = IndexLogstoreMappings._get_match_index_lst(None, ["my_index", "your_index"])
        self.assertListEqual(actual_match_index_lst, [])

        actual_match_index_lst = IndexLogstoreMappings._get_match_index_lst("index", None)
        self.assertListEqual(actual_match_index_lst, [])

        actual_match_index_lst = IndexLogstoreMappings._get_match_index_lst(None, None)
        self.assertListEqual(actual_match_index_lst, [])

        actual_match_index_lst = IndexLogstoreMappings._get_match_index_lst("index",
                                                                            ["my_index", "index", "your_index"])
        self.assertListEqual(actual_match_index_lst, ["index"])

        actual_match_index_lst = IndexLogstoreMappings._get_match_index_lst("index*",
                                                                            ["index", "index123", "inde"])
        self.assertListEqual(actual_match_index_lst, ["index", "index123"])


if __name__ == '__main__':
    unittest.main()
