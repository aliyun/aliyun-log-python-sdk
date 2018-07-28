#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.


import logging
import time
from multiprocessing import Pool

from elasticsearch import Elasticsearch

from .. import LogClient, LogException
from ..version import ES_MIGRATION_USER_AGENT
from .collection_task import CollectionTaskStatus, run_collection_task
from .collection_task_config import CollectionTaskConfig
from .index_logstore_mappings import IndexLogstoreMappings
from .mapping_index_converter import MappingIndexConverter
from .util import split_and_strip

results = []


def log_result(result):
    results.append(result)


class MigrationManager(object):
    """
    MigrationManager, migrate data from elasticsearch to aliyun log service

    :type hosts: string
    :param hosts: a comma-separated list of source ES nodes. e.g. "localhost:9200,other_host:9200"

    :type indexes: string
    :param indexes: a comma-separated list of source index names. e.g. "index1,index2"

    :type query: string
    :param query: used to filter docs, so that you can specify the docs you want to migrate. e.g. '{"query": {"match": {"title": "python"}}}'

    :type scroll: string
    :param scroll: specify how long a consistent view of the index should be maintained for scrolled search. e.g. "5m"

    :type endpoint: string
    :param endpoint: specify the endpoint of your log services. e.g. "cn-beijing.log.aliyuncs.com"

    :type project_name: string
    :param project_name: specify the project_name of your log services. e.g. "your_project"

    :type access_key_id: string
    :param access_key_id: specify the access_key_id of your account.

    :type access_key: string
    :param access_key: specify the access_key of your account.

    :type logstore_index_mappings: string
    :param logstore_index_mappings: specify the mappings of log service logstore and ES index. e.g. '{"logstore1": "my_index*", "logstore2": "index1,index2"}, "logstore3": "index3"}'

    :type pool_size: int
    :param pool_size: specify the size of process pool. e.g. 10

    :type time_reference: string
    :param time_reference: specify what ES doc's field to use as log's time field. e.g. "field1"

    :type source: string
    :param source: specify the value of log's source field. e.g. "your_source"

    :type topic: string
    :param topic: specify the value of log's topic field. e.g. "your_topic"

    :type wait_time_in_secs: int
    :param wait_time_in_secs: specify the waiting time between initialize aliyun log and executing data migration task. e.g. 60
    """

    def __init__(self, hosts=None, indexes=None, query=None, scroll="5m", endpoint=None, project_name=None,
                 access_key_id=None, access_key=None, logstore_index_mappings=None, pool_size=10, time_reference=None,
                 source=None, topic=None, wait_time_in_secs=60):
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
        self.wait_time_in_secs = wait_time_in_secs

    def migrate(self):
        es = Elasticsearch(split_and_strip(self.hosts, ","))
        log_client = LogClient(self.endpoint, self.access_key_id, self.access_key)
        log_client.set_user_agent(ES_MIGRATION_USER_AGENT)

        index_lst = self.get_index_lst(es, self.indexes)
        index_logstore_mappings = IndexLogstoreMappings(index_lst, self.logstore_index_mappings)

        self.init_aliyun_log(es, log_client, self.project_name, index_logstore_mappings, self.wait_time_in_secs)

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
                                          index_logstore_mappings=index_logstore_mappings,
                                          time_reference=self.time_reference,
                                          source=self.source,
                                          topic=self.topic)
            p.apply_async(func=run_collection_task, args=(config,), callback=log_result)

        p.close()
        p.join()

        self.logging_summary_info(shard_cnt)

    @classmethod
    def logging_summary_info(cls, shard_cnt):
        total_started_task_cnt = shard_cnt
        success_task_cnt = 0
        fail_task_cnt = 0
        doc_cnt = 0
        logging.info("========Tasks Info========")
        for res in results:
            logging.info(res)
            doc_cnt += res.count
            if res.status == CollectionTaskStatus.SUCCESS:
                success_task_cnt += 1
            else:
                fail_task_cnt += 1

        logging.info("========Summary========")
        logging.info("Total started task count: %d", total_started_task_cnt)
        logging.info("Successful task count: %d", success_task_cnt)
        logging.info("Failed task count: %d", fail_task_cnt)
        logging.info("Total collected documentation count: %d", doc_cnt)

    @classmethod
    def get_shard_count(cls, es, indexes, query=None):
        resp = es.count(index=indexes, body=query)
        return resp["_shards"]["total"]

    @classmethod
    def get_index_lst(cls, es, indexes):
        resp = es.indices.stats(index=indexes)
        return resp["indices"].keys()

    @classmethod
    def init_aliyun_log(cls, es, log_client, project_name, index_logstore_mappings, wait_time_in_secs):
        logging.info("Start to init aliyun log")
        cls._create_logstores(log_client, project_name, index_logstore_mappings)
        cls._create_index_configs(es, log_client, project_name, index_logstore_mappings)
        logging.info("Init aliyun log successfully")
        logging.info("Enter wating time, wait_time_in_secs=%d", wait_time_in_secs)
        time.sleep(wait_time_in_secs)
        logging.info("Exit wating time")

    @classmethod
    def _create_logstores(cls, log_client, project_name, index_logstore_mappings):
        logstores = index_logstore_mappings.get_all_logstores()
        for logstore in logstores:
            try:
                log_client.create_logstore(project_name=project_name, logstore_name=logstore)
            except LogException as e:
                if e.get_error_code() == "LogStoreAlreadyExist":
                    continue
                else:
                    raise

    @classmethod
    def _create_index_configs(cls, es, log_client, project_name, index_logstore_mappings):
        logstores = index_logstore_mappings.get_all_logstores()
        for logstore in logstores:
            indexes = index_logstore_mappings.get_indexes(logstore)
            first_index = True
            for index in indexes:
                resp = es.indices.get(index=index)
                for mapping in resp[index]["mappings"].values():
                    index_config = MappingIndexConverter.to_index_config(mapping)
                    if first_index:
                        try:
                            log_client.create_index(project_name, logstore, index_config)
                            first_index = False
                        except LogException as e:
                            if e.get_error_code() == "IndexAlreadyExist":
                                continue
                            else:
                                raise
                    else:
                        log_client.update_index(project_name, logstore, index_config)
