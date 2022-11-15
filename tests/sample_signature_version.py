# -*- coding: utf-8 -*-

from time import time, sleep
from aliyun.log import *

def operate(client, project ,logstore):
    # 创建project、logstore
    client.create_project(project, "")
    client.create_logstore(project, logstore)

    # logstore信息查询
    response = client.list_logstores(ListLogstoresRequest(project))
    print("total: " + str(response.get_total()))
    print("count: " + str(response.get_count()))
    for name in response.get_logstores():
        print("logstore name: " + name)
    sleep(60)

    # 数据发送，读取
    log_item_list = []
    for i in range(10):
        contents = [('index', str(i))]
        log_item = LogItem()
        log_item.set_time(int(time()))
        log_item.set_contents(contents)
        log_item_list.append(log_item)
    request = PutLogsRequest(project, logstore, "", "", log_item_list)
    response2 = client.put_logs(request)
    print(response2.log_print())

    listShardRes = client.list_shards(project, logstore)
    for shard in listShardRes.get_shards_info():
        shard_id = shard["shardID"]
        start_time = int(time() - 60)
        end_time = start_time + 60
        response3 = client.get_cursor(project, logstore, shard_id, start_time)
        start_cursor = response3.get_cursor()
        response4 = client.get_cursor(project, logstore, shard_id, end_time)
        end_cursor = response4.get_cursor()
        response5 = client.pull_logs(project, logstore, shard_id, start_cursor, 10, end_cursor)
        response5.log_print()

    # 删除project、logstore
    client.delete_logstore(project, logstore)
    client.delete_project(project)

def main():
    endpoint = ''
    region = ''  # 与endpoint对应的地域
    access_key_id = ''  # 使用您的阿里云访问密钥AccessKeyId
    access_key = ''  # 使用您的阿里云访问密钥AccessKeySecret
    project = ''  # 下面将创建的项目名称
    logstore = ''  # 下面将创建的logstore名称

    # 构建一个client
    client = LogClient(endpoint, access_key_id, access_key)

    # 正常使用v1 签名
    operate(client, project, logstore)

    # 使用v4签名，需要设置region
    print("=============== begin to use v4 sign ===============")
    client.set_sign_version('v4', region)
    operate(client, project, logstore)

if __name__ == '__main__':
    main()
