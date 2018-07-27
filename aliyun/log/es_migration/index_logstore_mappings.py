#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.


import json
import re

from .util import split_and_strip


class IndexLogstoreMappings(object):

    def __init__(self, index_lst=None, logstore_index_mappings=None):
        self.index_logstore_dct = {}
        self.logstore_indexes_dct = {}

        if not index_lst:
            return
        self.index_logstore_dct = {index: index for index in index_lst}

        logstore_index_dct = {}
        if logstore_index_mappings:
            logstore_index_dct = json.loads(logstore_index_mappings)
        self._update_dicts(logstore_index_dct)

    def _update_dicts(self, logstore_index_dct):
        all_indexes = self.index_logstore_dct.keys()

        for k, v in logstore_index_dct.items():
            indexes = split_and_strip(v, ",")
            index_lst = []
            for pattern in indexes:
                match_index_lst = self._get_match_indexes(pattern, all_indexes)
                index_lst.extend(match_index_lst)
                for index in match_index_lst:
                    if index in self.index_logstore_dct and \
                            self.index_logstore_dct[index] in logstore_index_dct:
                        raise Exception(
                            "index '%s' belongs to '%s' and '%s'" % (index, k, self.index_logstore_dct[index]))
                    self.index_logstore_dct[index] = k
            if index_lst:
                self.logstore_indexes_dct[k] = index_lst

        for v in self.index_logstore_dct.values():
            if v not in self.logstore_indexes_dct:
                self.logstore_indexes_dct[v] = [v]

    def get_logstore(self, index):
        if index in self.index_logstore_dct:
            return self.index_logstore_dct[index]
        return None

    def get_all_logstores(self):
        return self.logstore_indexes_dct.keys()

    def get_indexes(self, logstore):
        if logstore in self.logstore_indexes_dct:
            return self.logstore_indexes_dct[logstore]
        return []

    def get_all_indexes(self):
        return self.index_logstore_dct.keys()

    @classmethod
    def _get_match_indexes(cls, pattern, index_lst):
        if not pattern or not index_lst:
            return []
        if "*" in pattern:
            regex = re.compile(pattern.replace("*", ".*"))
            match_index_lst = [index for index in index_lst if re.match(regex, index)]
        else:
            match_index_lst = [index for index in index_lst if pattern == index]
        return match_index_lst
