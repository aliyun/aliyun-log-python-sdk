#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

import unittest

from aliyun.log.es_migration.logstore_index_mapper import LogstoreIndexMapper


class TestLogstoreIndexMapper(unittest.TestCase):

    def test_get_logstore(self):
        logstore_index_mappings = '''
            {
                "logstore1": "my_index*", 
                "logstore2": "a_index,b_index,c_index", 
                "logstore3": "index1",
                "logstore4": "xxx_*_yyy"
            }
            '''

        mapper = LogstoreIndexMapper(logstore_index_mappings)

        self.assertEqual(mapper.get_logstore("my_index123"), "logstore1")
        self.assertEqual(mapper.get_logstore("my_indexabcd"), "logstore1")
        self.assertEqual(mapper.get_logstore("my_index"), "logstore1")
        self.assertIsNone(mapper.get_logstore("my_inde"))

        self.assertEqual(mapper.get_logstore("a_index"), "logstore2")
        self.assertEqual(mapper.get_logstore("b_index"), "logstore2")
        self.assertEqual(mapper.get_logstore("c_index"), "logstore2")
        self.assertIsNone(mapper.get_logstore("d_index"))

        self.assertEqual(mapper.get_logstore("index1"), "logstore3")

        self.assertEqual(mapper.get_logstore("xxx__yyy"), "logstore4")
        self.assertEqual(mapper.get_logstore("xxx_123_yyy"), "logstore4")
        self.assertEqual(mapper.get_logstore("xxx_xxx_yyy_yyy"), "logstore4")
        self.assertIsNone(mapper.get_logstore("xxx_yyy"))
        self.assertIsNone(mapper.get_logstore("xxxyyy"))


if __name__ == '__main__':
    unittest.main()
