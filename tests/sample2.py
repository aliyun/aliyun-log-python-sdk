# encoding: utf-8

import time
from aliyun.log import *


def main():
    endpoint = ''       # 选择与上面步骤创建Project所属区域匹配的Endpoint
    accessKeyId = ''    # 使用你的阿里云访问密钥AccessKeyId
    accessKey = ''      # 使用你的阿里云访问密钥AccessKeySecret
    project = ''        # 上面步骤创建的项目名称
    logstore = ''       # 上面步骤创建的日志库名称

    # 重要提示：创建的logstore请配置为4个shard以便于后面测试通过

    # 构建一个client
    client = LogClient(endpoint, accessKeyId, accessKey)

    # list 所有的logstore
    req1 = ListLogstoresRequest(project)
    res1 = client.list_logstores(req1)
    res1.log_print()
    topic = ""
    source = ""

    # 发送10个数据包，每个数据包有10条log
    for i in range(10):
        logitemList = []  # LogItem list
        for j in range(10):
            contents = [('index', str(i * 10 + j))]
            logItem = LogItem()
            logItem.set_time(int(time.time()))
            logItem.set_contents(contents)
            logitemList.append(logItem)
        req2 = PutLogsRequest(project, logstore, topic, source, logitemList)
        res2 = client.put_logs(req2)
        res2.log_print()

    # list所有的shard，读取上1分钟写入的数据全部读取出来
    listShardRes = client.list_shards(project, logstore)
    for shard in listShardRes.get_shards_info():
        shard_id = shard["shardID"]
        start_time = int(time.time() - 60)
        end_time = start_time + 60
        res = client.get_cursor(project, logstore, shard_id, start_time)
        res.log_print()
        start_cursor = res.get_cursor()
        res = client.get_cursor(project, logstore, shard_id, end_time)
        end_cursor = res.get_cursor()

        while True:
            loggroup_count = 100  # 每次读取100个包
            res = client.pull_logs(project, logstore, shard_id, start_cursor, loggroup_count, end_cursor)
            res.log_print()
            next_cursor = res.get_next_cursor()
            if next_cursor == start_cursor:
                break
            start_cursor = next_cursor

    # 重要提示： 只有打开索引功能，才可以使用以下接口来查询数据
    time.sleep(60)
    topic = ""
    query = "index"
    From = int(time.time()) - 600
    To = int(time.time())
    res3 = None

    # 查询最近10分钟内，满足query条件的日志条数，如果执行结果不是完全正确，则进行重试
    while (res3 is None) or (not res3.is_completed()):
        req3 = GetHistogramsRequest(project, logstore, From, To, topic, query)
        res3 = client.get_histograms(req3)
    res3.log_print()

    # 获取满足query的日志条数
    total_log_count = res3.get_total_count()
    log_line = 10

    # 每次读取10条日志，将日志数据查询完，对于每一次查询，如果查询结果不是完全准确，则重试3次
    for offset in range(0, total_log_count, log_line):
        res4 = None
        for retry_time in range(0, 3):
            req4 = GetLogsRequest(project, logstore, From, To, topic, query, log_line, offset, False)
            res4 = client.get_logs(req4)
            if res4 is not None and res4.is_completed():
                break
            time.sleep(1)
        if res4 is not None:
            res4.log_print()
    listShardRes = client.list_shards(project, logstore)
    shard = listShardRes.get_shards_info()[0]

    # 分裂shard
    if shard["status"] == "readwrite":
        shard_id = shard["shardID"]
        inclusiveBeginKey = shard["inclusiveBeginKey"]
        midKey = inclusiveBeginKey[:-1] + str((int(inclusiveBeginKey[-1:])) + 1)
        client.split_shard(project, logstore, shard_id, midKey)

    # 合并shard
    shard = listShardRes.get_shards_info()[1]
    if shard["status"] == "readwrite":
        shard_id = shard["shardID"]
        client.merge_shard(project, logstore, shard_id)

    # 删除shard
    shard = listShardRes.get_shards_info()[-1]
    if shard["status"] == "readonly":
        shard_id = shard["shardID"]
        client.delete_shard(project, logstore, shard_id)


if __name__ == '__main__':
    main()
