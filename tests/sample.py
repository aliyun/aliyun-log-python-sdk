# encoding: utf-8
from __future__ import print_function

from aliyun.log.gethistogramsrequest import GetHistogramsRequest
from aliyun.log.getlogsrequest import GetLogsRequest
from aliyun.log.index_config import *
from aliyun.log.listlogstoresrequest import ListLogstoresRequest
from aliyun.log.listtopicsrequest import ListTopicsRequest
from aliyun.log.logclient import LogClient
from aliyun.log.logitem import LogItem
from aliyun.log.logtail_config_detail import *
from aliyun.log.machine_group_detail import *
from aliyun.log.putlogsrequest import PutLogsRequest
import time
import random
import os


# def log_enter_exit(fn):
#     def wrapped(*args, **kwargs):
#         print("** enter: ", fn.__name__)
#         ret = fn(*args, **kwargs)
#         print("** exit: ", fn.__name__)
#         return ret
#
#     return wrapped



def sample_put_logs(client, project, logstore):
    topic = 'TestTopic_2'
    source = ''
    contents = [
        ('key_1', 'key_1'),
        ('avg', '30')
    ]
    logitemList = []  # LogItem list
    logItem = LogItem()
    logItem.set_time(int(time.time()))
    logItem.set_contents(contents)
    for i in range(0, 1):
        logitemList.append(logItem)
    request = PutLogsRequest(project, logstore, topic, source, logitemList)

    response = client.put_logs(request)
    response.log_print()


# @log_enter_exit
def sample_pull_logs(client, project, logstore):
    res = client.get_cursor(project, logstore, 0, int(time.time() - 60))
    res.log_print()
    cursor = res.get_cursor()

    res = client.pull_logs(project, logstore, 0, cursor, 1)
    res.log_print()


# @log_enter_exit
def sample_list_logstores(client, project):
    request = ListLogstoresRequest(project)
    response = client.list_logstores(request)
    response.log_print()


# @log_enter_exit
def sample_list_topics(client, project, logstore):
    request = ListTopicsRequest(project, logstore, "", 2)
    response = client.list_topics(request)
    response.log_print()


# @log_enter_exit
def sample_get_logs(client, project, logstore):
    topic = 'TestTopic_2'
    From = int(time.time()) - 3600
    To = int(time.time())
    request = GetLogsRequest(project, logstore, From, To, topic)
    response = client.get_logs(request)
    response.log_print()


# @log_enter_exit
def sample_get_histograms(client, project, logstore):
    topic = 'TestTopic_2'
    From = int(time.time()) - 3600
    To = int(time.time())
    request = GetHistogramsRequest(project, logstore, From, To, topic)
    response = client.get_histograms(request)
    response.log_print()


# @log_enter_exit
def sample_machine_group(client, project, logstore):
    group_name = logstore + "-sample-group"

    machine_group = MachineGroupDetail(group_name, "ip", ["127.0.0.1", "127.0.0.2"], "Armory",
                                       {"externalName": "test-1", "groupTopic": "yyy"})

    res = client.create_machine_group(project, machine_group)
    res.log_print()

    res = client.list_machine_group(project)
    res.log_print()

    res = client.get_machine_group(project, group_name)
    res.log_print()


# @log_enter_exit
def sample_index(client, project, logstore):
    line_config = IndexLineConfig([" ", "\\t", "\\n", ","], False, ["key_1", "key_2"])

    key_config_list = {"key_1": IndexKeyConfig([",", "\t", ";"], True)}

    index_detail = IndexConfig(7, line_config, key_config_list)

    res = client.create_index(project, logstore, index_detail)

    res.log_print()

    key_config_list = {"key_1": IndexKeyConfig([",", "\t", ";"], True)}

    index_detail = IndexConfig(7, line_config, key_config_list)

    res = client.update_index(project, logstore, index_detail)

    res.log_print()

    res = client.get_index_config(project, logstore)
    res.log_print()


# @log_enter_exit
def sample_logtail_config(client, project, logstore):
    logtail_config_name = logstore + "-stt1-logtail"
    logtail_config = CommonRegLogConfigDetail(logtail_config_name, logstore,
                                              "http://cn-hangzhou-devcommon-intranet.sls.aliyuncs.com", "/apsara/xxx",
                                              "*.LOG",
                                              "%Y-%m-%d %H:%M:%S", "xxx.*", "(.*)(.*)", ["time", "value"])

    res = client.create_logtail_config(project, logtail_config)
    res.log_print()

    res = client.get_logtail_config(project, logtail_config_name)
    res.log_print()

    res = client.delete_logtail_config(project, logtail_config_name)
    res.log_print()


# @log_enter_exit
def sample_apply_config(client, project, logstore):
    logtail_config_name = logstore + "-stt1-logtail"
    logtail_config = CommonRegLogConfigDetail(logtail_config_name, logstore,
                                              "http://cn-hangzhou-devcommon-intranet.sls.aliyuncs.com", "/apsara/xxx",
                                              "*.LOG",
                                              "%Y-%m-%d %H:%M:%S", "xxx.*", "(.*)(.*)", ["time", "value"])

    client.create_logtail_config(project, logtail_config)

    group_name = logstore + "-sample-group"
    res = client.get_machine_group_applied_configs(project, group_name)
    res.log_print()

    res = client.get_config_applied_machine_groups(project, logtail_config_name)
    res.log_print()

    res = client.apply_config_to_machine_group(project, logtail_config_name, group_name)
    res.log_print()


# @log_enter_exit
def sample_logstore(client, project, logstore):
    res = client.create_logstore(project, logstore, 1, 1)
    res.log_print()

    res = client.update_logstore(project, logstore, 2, 1)
    res.log_print()

    res = client.list_logstore(project, logstore)
    res.log_print()

    client.delete_logstore(project, logstore)


def sample_cleanup(client, project, logstore):
    logtail_config_name = logstore + "-stt1-logtail"
    group_name = logstore + "-sample-group"

    # delete all created items
    client.delete_machine_group(project, group_name)
    client.delete_logtail_config(project, logtail_config_name)
    client.delete_logstore(project, logstore)


def main():
    endpoint = os.environ.get('ALIYUN-LOG-SAMPLE-ENDPOINT', 'cn-hangzhou.log.aliyuncs.com')
    accessKeyId = os.environ.get('ALIYUN-LOG-SAMPLE-ACCESSID', '')
    accessKey = os.environ.get('ALIYUN-LOG-SAMPLE-ACCESSKEY', '')
    project = os.environ.get('ALIYUN-LOG-SAMPLE-PROJECT', '')
    logstore = os.environ.get('ALIYUN-LOG-SAMPLE-LOGSTORE', '')
    token = ""

    if not logstore:
        logstore = 'sdk-test' + str(random.randint(1, 1000))

    assert endpoint and accessKeyId and accessKey and project, ValueError("endpoint/access_id/key and "
                                                                          "project cannot be empty")

    client = LogClient(endpoint, accessKeyId, accessKey, token)

    sample_logstore(client, project, logstore)
    time.sleep(2)

    client.create_logstore(project, logstore, 1, 1)
    time.sleep(2)

    sample_list_logstores(client, project)
    sample_logtail_config(client, project, logstore)
    sample_machine_group(client, project, logstore)
    sample_apply_config(client, project, logstore)
    sample_index(client, project, logstore)
    time.sleep(40)

    sample_list_topics(client, project, logstore)
    time.sleep(40)
    sample_put_logs(client, project, logstore)

    sample_pull_logs(client, project, logstore)

    time.sleep(40)
    sample_get_logs(client, project, logstore)

    time.sleep(10)
    sample_cleanup(client, project, logstore)


if __name__ == '__main__':
    main()
