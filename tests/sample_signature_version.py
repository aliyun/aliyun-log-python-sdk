# -*- coding: utf-8 -*-

import os
from time import time, sleep
from aliyun.log import *

def operate(client, project ,logstore):
    # 创建project、logstore
    # client.create_project(project, '')
    # client.create_logstore(project, logstore)

    # logstore信息查询
    response = client.list_logstores(ListLogstoresRequest(project))
    print('total: ' + str(response.get_total()))
    print('count: ' + str(response.get_count()))
    for name in response.get_logstores():
        print('logstore name: ' + name)
    # sleep(60)

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
    print(response2.log_print())

    listShardRes = client.list_shards(project, logstore)
    for shard in listShardRes.get_shards_info():
        shard_id = shard['shardID']
        start_time = int(time() - 60)
        end_time = start_time + 60
        response3 = client.get_cursor(project, logstore, shard_id, start_time)
        start_cursor = response3.get_cursor()
        response4 = client.get_cursor(project, logstore, shard_id, end_time)
        end_cursor = response4.get_cursor()
        response5 = client.pull_logs(project, logstore, shard_id, start_cursor, 10, end_cursor)
        response5.log_print()

    # 删除project、logstore
    # client.delete_logstore(project, logstore)
    # client.delete_project(project)

def main():
    endpoint = os.environ.get('ALIYUN_LOG_SAMPLE_ENDPOINT', '')
    access_key_id = os.environ.get('ALIYUN_LOG_SAMPLE_ACCESSID', '')
    access_key = os.environ.get('ALIYUN_LOG_SAMPLE_ACCESSKEY', '')
    region = os.environ.get('ALIYUN_LOG_SAMPLE_REGION', '')
    project = os.environ.get('ALIYUN_LOG_SAMPLE_PROJECT', '')
    logstore = os.environ.get('ALIYUN_LOG_SAMPLE_LOGSTORE', '')

    # 构建一个client
    client = LogClient(endpoint, access_key_id, access_key, None, None, auth.AUTH_VERSION_4, region)

    # 使用v4签名进行操作
    operate(client, project, logstore)

if __name__ == '__main__':
    main()
