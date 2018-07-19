#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.


class CollectionTaskConfig(object):

    def __init__(self, task_id=None, slice_id=None, slice_max=None, hosts=None, indexes=None, query=None, scroll=None,
                 endpoint=None, project=None, access_key_id=None, access_key=None, index_logstore_mappings=None,
                 time_reference=None, source=None, topic=None):
        self.task_id = task_id
        self.slice_id = slice_id
        self.slice_max = slice_max
        self.hosts = hosts
        self.indexes = indexes
        self.query = query
        self.scroll = scroll
        self.endpoint = endpoint
        self.project = project
        self.access_key_id = access_key_id
        self.access_key = access_key
        self.index_logstore_mappings = index_logstore_mappings
        self.time_reference = time_reference
        self.source = source
        self.topic = topic
