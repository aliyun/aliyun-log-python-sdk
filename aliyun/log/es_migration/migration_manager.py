#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.


from multiprocessing import Pool

from elasticsearch import Elasticsearch

from aliyun.log.es_migration.collection_task import run_collection_task
from aliyun.log.es_migration.collection_task_config import CollectionTaskConfig
from aliyun.log.es_migration.logstore_index_mapper import LogstoreIndexMapper
from aliyun.log.es_migration.util import split_and_strip

results = []


def log_result(result):
    results.append(result)


class MigrationManager(object):

    def __init__(self, hosts=None, indexes=None, query=None, scroll="5m", endpoint=None, project_name=None,
                 access_key_id=None, access_key=None, logstore_index_mappings=None, pool_size=10, time_reference=None,
                 source=None, topic=None):
        self.hosts = hosts
        self.indexes = indexes
        self.query = query
        self.scroll = scroll
        self.endpoint = endpoint
        self.project_name = project_name
        self.access_key_id = access_key_id
        self.access_key = access_key
        self.logstore_index_mappings = logstore_index_mappings
        self.pool_size = pool_size
        self.time_reference = time_reference
        self.source = source
        self.topic = topic

    def migrate(self):
        logstore_index_mapper = LogstoreIndexMapper(self.logstore_index_mappings)

        es = Elasticsearch(split_and_strip(self.hosts))
        shard_cnt = self.get_shard_count(es, self.indexes, self.query)

        p = Pool(min(shard_cnt, self.pool_size))

        for i in range(shard_cnt):
            config = CollectionTaskConfig(task_id=i,
                                          slice_id=i,
                                          slice_max=shard_cnt,
                                          hosts=self.hosts,
                                          indexes=self.indexes,
                                          query=self.query,
                                          scroll=self.scroll,
                                          endpoint=self.endpoint,
                                          project=self.project_name,
                                          access_key_id=self.access_key_id,
                                          access_key=self.access_key,
                                          logstore_index_mapper=logstore_index_mapper,
                                          time_reference=self.time_reference,
                                          source=self.source,
                                          topic=self.topic)
            p.apply_async(func=run_collection_task, args=(config,), callback=log_result)

        p.close()
        p.join()
        for res in results:
            print res

    @classmethod
    def get_shard_count(cls, client, indexes, query=None):
        resp = client.count(index=indexes, body=query)
        return resp["_shards"]["total"]
