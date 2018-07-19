#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.


import os
from multiprocessing import Pool

from elasticsearch import Elasticsearch

from aliyun.log.es_migration.collection_task import run_collection_task
from aliyun.log.es_migration.collection_task_config import CollectionTaskConfig
from aliyun.log.es_migration.util import split_and_strip
from aliyun.log.es_migration.logstore_index_mapper import LogstoreIndexMapper

DEFAULT_POOL_SIZE = 10

results = []


def log_result(result):
    results.append(result)


def es_migration(hosts=None, indexes=None, query=None, scroll="5m", endpoint=None, project_name=None,
                 logstore_index_mappings=None, pool_size=DEFAULT_POOL_SIZE, time_reference=None, source=None,
                 topic=None):
    logstore_index_mapper = LogstoreIndexMapper(logstore_index_mappings)

    es = Elasticsearch(split_and_strip(hosts))
    shard_cnt = get_shard_count(es, indexes, query)

    p = Pool(min(shard_cnt, pool_size))

    for i in range(shard_cnt):
        config = CollectionTaskConfig(task_id=i, slice_id=i, slice_max=shard_cnt, hosts=hosts, indexes=indexes,
                                      endpoint=endpoint, project=project_name,
                                      access_key_id=os.getenv("access_key_id"), access_key=os.getenv("access_key"),
                                      logstore_index_mapper=logstore_index_mapper)
        p.apply_async(func=run_collection_task, args=(config,), callback=log_result)

    p.close()
    p.join()
    for res in results:
        print res


def get_shard_count(client, indexes, query=None):
    resp = client.count(index=indexes, body=query)
    return resp["_shards"]["total"]


if __name__ == '__main__':
    es_migration(hosts="localhost:9200", indexes="movies", endpoint=os.getenv("endpoint"),
                 project_name=os.getenv("project_name"))
