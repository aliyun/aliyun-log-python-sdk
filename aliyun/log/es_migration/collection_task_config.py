#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.


class CollectionTaskConfig(object):
    DEFAULT_SCROLL = "5m"

    DEFAULT_SOURCE = ""

    DEFAULT_TOPIC = ""

    def __init__(self, task_id=None, slice_id=None, slice_max=None, hosts=None, indexes=None, query=None,
                 scroll=DEFAULT_SCROLL, project_name=None, logstore_index_mappings=None, time_reference=None,
                 source=DEFAULT_SOURCE, topic=DEFAULT_TOPIC):
        self.task_id = task_id
        self.slice_id = slice_id
        self.slice_max = slice_max
        self.hosts = hosts
        self.indexes = indexes
        self.query = query
        self.scroll = scroll
        self.project_name = project_name
        self.logstore_index_mappings = logstore_index_mappings
        self.time_reference = time_reference
        self.source = source
        self.topic = topic
