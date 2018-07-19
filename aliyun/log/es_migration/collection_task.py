#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

import logging
import time
from enum import Enum

from elasticsearch import Elasticsearch

from aliyun.log.es_migration.util import split_and_strip


def run_collection_task(config):
    start_time = time.time()
    try:
        task = CollectionTask(config.task_id, config.slice_id, config.slice_max, config.hosts, config.indexes,
                              config.query, config.scroll, config.project_name, config.logstore_index_mappings,
                              config.time_reference, config.source, config.topic, start_time)
        return task.collect()
    except Exception as e:
        logging.exception("Failed to run collection task. e=%s", e)
        return build_collection_task_result(config.task_id, config.slice_id, config.slice_max,
                                            config.hosts, config.indexes, config.query,
                                            config.project_name, start_time,
                                            CollectionTaskStatus.FAIL_NO_RETRY, e.message)


def build_collection_task_result(task_id, slice_id, slice_max, hosts, indexes, query, project_name, start_time, status,
                                 message=None):
    cur_time = time.time()
    time_cost_in_seconds = cur_time - start_time
    return CollectionTaskResult(task_id, slice_id, slice_max, hosts, indexes, query, project_name, time_cost_in_seconds,
                                status, message)


class CollectionTaskResult(object):

    def __init__(self, task_id, slice_id, slice_max, hosts, indexes, query, project_name, time_cost_in_seconds, status,
                 message=None):
        self.task_id = task_id
        self.slice_id = slice_id
        self.slice_max = slice_max
        self.hosts = hosts
        self.indexes = indexes
        self.query = query
        self.project_name = project_name
        self.time_cost_in_seconds = time_cost_in_seconds
        self.status = status
        self.message = message

    def __str__(self):
        return "task_id=%s, slice_id=%s, slice_max=%s, hosts=%s, indexes=%s, " \
               "query=%s, project_name=%s, time_cost_in_seconds=%s, status=%s, message=%s" % \
               (self.task_id, self.slice_id, self.slice_max, self.hosts, self.indexes, self.query, self.project_name,
                self.time_cost_in_seconds, self.status, self.message)


class CollectionTaskStatus(Enum):
    SUCCESS = "SUCCESS"
    FAIL_RETRY = "FAIL_RETRY"
    FAIL_NO_RETRY = "FAIL_NO_RETRY"


class CollectionTask(object):
    DEFAULT_SIZE = 1000

    def __init__(self, task_id, slice_id, slice_max, hosts, indexes, query, scroll, project_name,
                 logstore_index_mappings, time_reference, source, topic, start_time):
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
        self.start_time = start_time

    def collect(self):
        hosts = split_and_strip(self.hosts)
        es = Elasticsearch(hosts)

        query = self.query.copy() if self.query else {}
        query["sort"] = "_doc"

        # initial search
        resp = es.search(index=self.indexes, body=query, scroll=self.scroll, size=self.DEFAULT_SIZE)

        scroll_id = resp.get("_scroll_id")
        if scroll_id is None:
            return self._build_collection_task_result(CollectionTaskStatus.SUCCESS)

        try:
            first_run = True
            while True:
                # if we didn't set search_type to scan initial search contains data
                if first_run:
                    first_run = False
                else:
                    resp = es.scroll(scroll_id, scroll=self.scroll)

                for hit in resp['hits']['hits']:
                    print hit

                # check if we have any errrors
                if resp["_shards"]["successful"] < resp["_shards"]["total"]:
                    msg = "Scroll request has only succeeded on %d shards out of %d." % \
                          (resp["_shards"]["successful"], resp["_shards"]["total"])
                    logging.warning(msg)
                    return self._build_collection_task_result(CollectionTaskStatus.SUCCESS, msg)
                scroll_id = resp.get('_scroll_id')
                # end of scroll
                if scroll_id is None or not resp['hits']['hits']:
                    break

            return self._build_collection_task_result(CollectionTaskStatus.SUCCESS)
        finally:
            if scroll_id:
                es.clear_scroll(body={'scroll_id': [scroll_id]}, ignore=(404,))

    def _build_collection_task_result(self, status, message=None):
        cur_time = time.time()
        time_cost_in_seconds = cur_time - self.start_time
        return CollectionTaskResult(self.task_id, self.slice_id, self.slice_max, self.hosts, self.indexes, self.query,
                                    self.project_name, time_cost_in_seconds, status, message)
