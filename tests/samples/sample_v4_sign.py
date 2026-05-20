#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

import os
from time import time, sleep

from aliyun.log import *
from aliyun.log.auth import *


def main():
    access_key_id = os.environ.get('ALIYUN_LOG_SAMPLE_ACCESSID', '')
    access_key = os.environ.get('ALIYUN_LOG_SAMPLE_ACCESSKEY', '')
    region = os.environ.get('ALIYUN_LOG_SAMPLE_REGION', '')
    project = os.environ.get('ALIYUN_LOG_SAMPLE_PROJECT', 'test-project-1')
    endpoint = os.environ.get('ALIYUN_LOG_SAMPLE_ENDPOINT', '')
    logstore = os.environ.get('ALIYUN_LOG_SAMPLE_LOGSTORE', 'test-logstore-1')

    client = LogClient(endpoint, access_key_id, access_key, auth_version=AUTH_VERSION_4, region=region)

    client.create_project(project, '')
    client.create_logstore(project, logstore)

    response = client.list_logstores(ListLogstoresRequest(project))
    print('total: ' + str(response.get_total()))
    print('count: ' + str(response.get_count()))
    for name in response.get_logstores():
        print('logstore name: ' + name)
    sleep(60)

    # 数据发送，读取
    log_item_list = []
    for i in range(10):
        contents = [('index', str(i))]
        log_item = LogItem()
        log_item.set_time(int(time()))
        log_item.set_contents(contents)
        log_item_list.append(log_item)
    request = PutLogsRequest(project, logstore, '', '', log_item_list)
    response2 = client.put_logs(request)
    response2.log_print()

    list_shard_resp = client.list_shards(project, logstore)
    for shard in list_shard_resp.get_shards_info():
        shard_id = shard['shardID']
        response3 = client.get_cursor(project, logstore, shard_id, 'begin')
        response4 = client.get_cursor(project, logstore, shard_id, 'end')
        start_cursor = response3.get_cursor()
        end_cursor = response4.get_cursor()
        response5 = client.pull_logs(project, logstore, shard_id, start_cursor, 10, end_cursor)
        response5.log_print()

    client.delete_logstore(project, logstore)
    client.delete_project(project)


if __name__ == '__main__':
    main()
