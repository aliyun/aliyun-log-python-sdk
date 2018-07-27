#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.


import json

from .. import LogItem
from ..util import parse_timestamp


class DocLogItemConverter(object):
    INDEX_FIELD = "_index"

    TYPE_FIELD = "_type"

    ID_FIELD = "_id"

    SOURCE_FIELD = "_source"

    TAG_PREFIX = "__tag__"

    @classmethod
    def to_log_item(cls, doc, time_reference):
        log_item = LogItem()
        cls._add_index(doc, log_item)
        cls._add_type(doc, log_item)
        cls._add_id(doc, log_item)
        cls._add_source(doc, log_item, time_reference)
        return log_item

    @classmethod
    def get_index(cls, doc):
        return doc[cls.INDEX_FIELD]

    @classmethod
    def _add_index(cls, doc, log_item):
        if cls.INDEX_FIELD not in doc:
            return
        log_item.push_back("%s:%s" % (cls.TAG_PREFIX, cls.INDEX_FIELD), doc[cls.INDEX_FIELD])

    @classmethod
    def _add_type(cls, doc, log_item):
        if cls.TYPE_FIELD not in doc:
            return
        log_item.push_back("%s:%s" % (cls.TAG_PREFIX, cls.TYPE_FIELD), doc[cls.TYPE_FIELD])

    @classmethod
    def _add_id(cls, doc, log_item):
        if cls.ID_FIELD not in doc:
            return
        log_item.push_back(cls.ID_FIELD, str(doc[cls.ID_FIELD]))

    @classmethod
    def _add_source(cls, doc, log_item, time_reference):
        if cls.SOURCE_FIELD not in doc:
            return
        for k, v in doc[cls.SOURCE_FIELD].items():
            if k == time_reference:
                log_item.set_time(parse_timestamp(v))
                continue
            if isinstance(v, dict):
                log_item.push_back(k, json.dumps(v))
            else:
                log_item.push_back(k, str(v))
