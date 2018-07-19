#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.


import json
import re
import string

from aliyun.log.es_migration.util import split_and_strip


class IndexLogstoreMappings(object):

    def __init__(self, index_lst=None, logstore_index_mappings=None):
        self.index_logstore_dct = {}

        if not index_lst:
            return
        self.index_logstore_dct = {index: index for index in index_lst}

        if not logstore_index_mappings:
            return
        logstore_index_dct = json.loads(logstore_index_mappings)
        self.index_logstore_dct = self._update_index_logstore_dct(self.index_logstore_dct, logstore_index_dct)

    @classmethod
    def _update_index_logstore_dct(cls, index_logstore_dct, logstore_index_dct):
        index_lst = index_logstore_dct.keys()
        for k, v in logstore_index_dct.iteritems():
            indexes = split_and_strip(v, ",")
            for pattern in indexes:
                match_index_lst = cls._get_match_index_lst(pattern, index_lst)
                for index in match_index_lst:
                    index_logstore_dct[index] = k
        return index_logstore_dct

    @classmethod
    def _get_match_index_lst(cls, pattern, index_lst):
        if not pattern or not index_lst:
            return []
        if string.find(pattern, "*") != -1:
            regex = re.compile(string.replace(pattern, "*", ".*"))
            match_index_lst = [index for index in index_lst if re.match(regex, index)]
        else:
            match_index_lst = [index for index in index_lst if pattern == index]
        return match_index_lst

    def get_logstore(self, index):
        if index in self.index_logstore_dct:
            return self.index_logstore_dct[index]
        return None
