# encoding: utf-8
from __future__ import print_function

from aliyun.log import *
from aliyun.log.util import base64_encodestring
from random import randint
import time
import os
from datetime import datetime

def sample_put_logs(client, project, logstore, compress=False):
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
    request = PutLogsRequest(project, logstore, topic, source, logitemList, compress=compress)

    response = client.put_logs(request)
    response.log_print()

    time.sleep(5)

    # check cursor time
    res = client.get_end_cursor(project, logstore, 0)
    end_cursor = res.get_cursor()

    res = client.get_cursor_time(project, logstore, 0, end_cursor)
    res.log_print()

    res = client.get_previous_cursor_time(project, logstore, 0, end_cursor)
    res.log_print()


# @log_enter_exit
def sample_pull_logs(client, project, logstore, compress=False):
    res = client.get_cursor(project, logstore, 0, int(time.time() - 60))
    res.log_print()
    cursor = res.get_cursor()

    res = client.pull_logs(project, logstore, 0, cursor, 1, compress=compress)
    res.log_print()

    # test readable start time
    res = client.get_cursor(project, logstore, 0,
                            datetime.fromtimestamp(int(time.time() - 60)).strftime('%Y-%m-%d %H:%M:%S'))
    res.log_print()

    # test pull_log
    res = client.pull_log(project, logstore, 0,
                            datetime.fromtimestamp(int(time.time() - 60)).strftime('%Y-%m-%d %H:%M:%S'),
                            datetime.fromtimestamp(int(time.time())).strftime('%Y-%m-%d %H:%M:%S'))
    for x in res:
        x.log_print()

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

    # test formatted time
    From = datetime.fromtimestamp(From).strftime('%Y-%m-%d %H:%M:%S')
    To = datetime.fromtimestamp(To).strftime('%Y-%m-%d %H:%M:%S')
    request = GetLogsRequest(project, logstore, From, To, topic)
    response = client.get_logs(request)
    response.log_print()

    res = client.get_log(project, logstore, From, To, topic)
    res.log_print()

    res = client.get_log_all(project, logstore, From, To, topic)
    for x in res:
        x.log_print()

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

    # list group
    res = client.list_machine_group(project)
    res.log_print()

    # list all
    res = client.list_machine_group(project, size=-1)
    res.log_print()

    res = client.get_machine_group(project, group_name)
    res.log_print()

    # list achine
    res = client.list_machines(project, group_name)
    res.log_print()

    # list all
    res = client.list_machines(project, group_name, size=-1)
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
    logtail_config = SimpleFileConfigDetail(logstore, logtail_config_name, "/apsara/xxx", "*.LOG")

    res = client.create_logtail_config(project, logtail_config)
    res.log_print()

    # test list
    res = client.list_logtail_config(project)
    res.log_print()

    # test list all
    res = client.list_logtail_config(project, size=-1)
    res.log_print()

    res = client.get_logtail_config(project, logtail_config_name)
    res.log_print()

    res = client.delete_logtail_config(project, logtail_config_name)
    res.log_print()


# @log_enter_exit
def sample_apply_config(client, project, logstore):
    logtail_config_name = logstore + "-stt1-logtail"
    logtail_config = SimpleFileConfigDetail(logstore, logtail_config_name, "/apsara/xxx", "*.LOG")

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
    res = client.create_logstore(project, logstore, 1, 1, True)
    res.log_print()

    res = client.update_logstore(project, logstore, 2)
    res.log_print()

    res = client.list_logstore(project, logstore)
    res.log_print()

    # list all
    res = client.list_logstore(project, logstore, size=-1)
    res.log_print()

    client.delete_logstore(project, logstore)


def sample_cleanup(client, project, logstore, delete_project=False):
    logtail_config_name = logstore + "-stt1-logtail"
    group_name = logstore + "-sample-group"

    # delete all created items
    try:
        client.delete_machine_group(project, group_name)
    except Exception as ex:
        print("ignore error when cleaning up: ", ex)
    try:
        client.delete_logtail_config(project, logtail_config_name)
    except Exception as ex:
        print("ignore error when cleaning up: ", ex)

    try:
        client.delete_logstore(project, logstore)
    except Exception as ex:
        print("ignore error when cleaning up: ", ex)

    if delete_project:
        time.sleep(30)
        client.delete_project(project)


def sample_crud_consumer_group(client, project, logstore, consumer_group):
    consumer_name1 = 'consumer_A'

    ret = client.create_consumer_group(project, logstore, consumer_group, 30, False)
    ret.log_print()

    ret = client.list_consumer_group(project, logstore)
    ret.log_print()

    time.sleep(60)
    ret = client.heart_beat(project, logstore, consumer_group, consumer_name1, [])
    ret.log_print()

    ret = client.update_check_point(project, logstore, consumer_group, 0, base64_encodestring('0'),
                                    consumer_name1, force_success=True)
    ret.log_print()

    ret = client.get_check_point(project, logstore, consumer_group)
    ret.log_print()

    ret = client.delete_consumer_group(project, logstore, consumer_group)
    ret.log_print()


def sample_get_project_log(client,project,logstore):
    req = GetProjectLogsRequest(project,"select count(1) from %s where __date__ >'2017-11-10 00:00:00' and __date__ < '2017-11-13 00:00:00'" %(logstore));
    res = client.get_project_logs(req)
    res.log_print()


def main():
    endpoint = os.environ.get('ALIYUN_LOG_SAMPLE_ENDPOINT', 'cn-hangzhou.log.aliyuncs.com')
    accessKeyId = os.environ.get('ALIYUN_LOG_SAMPLE_ACCESSID', '')
    accessKey = os.environ.get('ALIYUN_LOG_SAMPLE_ACCESSKEY', '')
    logstore = os.environ.get('ALIYUN_LOG_SAMPLE_LOGSTORE', '')
    consumer_group = 'sample_consumer_group_1'
    token = ""

    project = 'python-sdk-test' + str(time.time()).replace('.', '-')

    if not logstore:
        logstore = 'sdk-test' + str(time.time()).replace('.', '_')

    assert endpoint and accessKeyId and accessKey and project, ValueError("endpoint/access_id/key and "
                                                                          "project cannot be empty")

    client = LogClient(endpoint, accessKeyId, accessKey, token)

    # test list project
    ret = client.list_project()
    print("**project count:", ret.get_count())

    # test list project all
    ret = client.list_project(size=-1)
    print("**project count:", ret.get_count())

    client.create_project(project, "SDK test")
    time.sleep(10)

    #
    project_new = project + str(randint(1, 10000)) + "-copied"

    try:
        sample_logstore(client, project, logstore)
        time.sleep(40)

        client.create_logstore(project, logstore, 1, 1)
        time.sleep(40)

        sample_list_logstores(client, project)
        sample_logtail_config(client, project, logstore)
        time.sleep(10)

        sample_machine_group(client, project, logstore)
        sample_apply_config(client, project, logstore)
        sample_index(client, project, logstore)
        time.sleep(40)

        sample_list_topics(client, project, logstore)
        time.sleep(40)
        sample_put_logs(client, project, logstore)
        sample_put_logs(client, project, logstore, compress=True)

        sample_pull_logs(client, project, logstore)
        sample_pull_logs(client, project, logstore, compress=True)

        time.sleep(40)
        sample_get_logs(client, project, logstore)
        sample_get_project_log(client,project,logstore)

        time.sleep(10)
        sample_crud_consumer_group(client, project, logstore, consumer_group)
        time.sleep(10)

        # test copy project
        try:
            client.copy_project(project, project_new, copy_machine_group=True)
            client.copy_logstore(project, logstore, logstore+"_copied", to_project=project_new)
        except Exception as ex:
            print(ex)
        finally:
            time.sleep(60)
            sample_cleanup(client, project_new, logstore, delete_project=True)
    finally:
        sample_cleanup(client, project, logstore, delete_project=True)


if __name__ == '__main__':
    main()
