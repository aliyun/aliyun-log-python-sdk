import logging

from multiprocessing import Pool

from elasticsearch import Elasticsearch

from aliyun.log.es_migration.collection_task import run_collection_task
from aliyun.log.es_migration.collection_task_config import CollectionTaskConfig

from aliyun.log.es_migration.util import split_and_strip

result_list = []


def log_result(result):
    result_list.append(result)


def do_run(hosts_str, indexes, query=None):
    hosts = split_and_strip(hosts_str)
    es = Elasticsearch(hosts)

    shard_cnt = get_shard_count(es, indexes, query)

    p = Pool(10)
    for i in range(shard_cnt):
        config = CollectionTaskConfig(task_id=i, slice_id=i, slice_max=shard_cnt, hosts=hosts_str, indexes=indexes)
        p.apply_async(func=run_collection_task, args=(config,), callback=log_result)

    p.close()
    p.join()
    for res in result_list:
        print res


def get_shard_count(client, indexes, query=None):
    resp = client.count(index=indexes, body=query)
    return resp["_shards"]["total"]


try:
    do_run("localhost:9200", "movies")
except Exception as e:
    logging.exception(e)
