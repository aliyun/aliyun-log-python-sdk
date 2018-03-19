# encoding: utf-8
from __future__ import print_function

from aliyun.log import *
import time
import os
import json
from aliyun.log.logtail_config_detail import LogtailConfigGenerator
import os


def clean_project(client, project):

    print("#" * 1000)
    print("*** start to cleanup project: ", project)

    print("*** start to delete logtail config")
    stores = client.list_logstores(ListLogstoresRequest(project))
    n = 0
    logtails = client.list_logtail_config(project, size=-1)
    for logtail in logtails.get_configs():
        try:
            ret = client.delete_logtail_config(project, logtail)
            print(ret)
            n += 1
        except LogException as ex:
            print("skip deleting config for", ex)
    print("### deleted {0} logtail config".format(n))

    print("*** start to delete logstore")
    for l in stores.get_logstores():
        client.delete_logstore(project, l)
    print("### deleted {0} log store".format(len(stores.get_logstores())))

    # delete project
    client.delete_project(project)


def test_logtail_config(client, project):
    dir_path = os.sep.join([os.path.dirname(__file__), "data"])
    file_names = [
        'simple_1', 'simple_2', 'simple_3', 'simple_4_docker',
        'feitian_1', 'feitian_2',
        'json_1', 'json_2', 'json_3', 'json_4_docker',
        'ngnix_1',
        'reg_1', 'reg_2', 'reg_3', 'reg_4_docker',
        'sep_1', 'sep_2', 'sep_3','sep_4_docker',
        'syslog_1',
        'docker-stdout-config', 'mysql-binlog-config',
        'mysql-rawsql-config', 'nginx-status-config'
    ]

    for file_name in file_names:
        json_path = os.sep.join([dir_path, file_name + '.json'])
        with open(json_path, "r") as f:
            json_value = json.load(f)
            detail = LogtailConfigGenerator.generate_config(json_value)
            print("****create config", file_name)
            res = client.create_logtail_config(project, detail)
            res.log_print()

    time.sleep(20)
    for config_name in file_names:
        print("****get config", config_name)
        res = client.get_logtail_config(project, config_name)
        res.log_print()

    for file_name in file_names:
        json_path = os.sep.join([dir_path, file_name + '.json'])
        with open(json_path, "r") as f:
            json_value = json.load(f)
            detail = LogtailConfigGenerator.generate_config(json_value)
            print("****update config", file_name)
            res = client.update_logtail_config(project, detail)
            res.log_print()

def main():
    endpoint = os.environ.get('ALIYUN_LOG_SAMPLE_ENDPOINT', '')
    accessKeyId = os.environ.get('ALIYUN_LOG_SAMPLE_ACCESSID', '')
    accessKey = os.environ.get('ALIYUN_LOG_SAMPLE_ACCESSKEY', '')

    project = 'python-sdk-test' + str(time.time()).replace('.', '-')
    logstore = 'logstore'

    assert endpoint and accessKeyId and accessKey, ValueError("endpoint/access_id/key cannot be empty")

    client = LogClient(endpoint, accessKeyId, accessKey, "")

    print("****create project", project)
    client.create_project(project, "SDK test")
    time.sleep(10)

    try:
        print("****create logstore", logstore)
        client.create_logstore(project, logstore, 1, 1)
        time.sleep(40)

        test_logtail_config(client, project)
    finally:
        clean_project(client, project)


if __name__ == '__main__':
    main()
