# encoding: utf-8
from __future__ import print_function

import json

from aliyun.log import *


def create_metric_store(client, project_name, logstore_name, substore_name, ttl, keys):
    res = client.create_logstore(project_name, logstore_name, ttl=ttl, telemetry_type='Metrics')
    res.log_print()
    res = client.create_substore(project_name, logstore_name, substore_name, ttl=ttl,
                                 keys=keys, sorted_key_count=1, time_index=2)
    res.log_print()


def main():
    endpoint = 'cn-hangzhou.log.aliyuncs.com'
    access_key_id = ''
    access_key = ''
    project_name = 'test-hangzhou-b'
    logstore_name = 'test_metricstore'

    substore_name = 'prom'
    ttl = 30
    keys = [
        {'name': '__name__', 'type': 'text'},
        {'name': '__labels__', 'type': 'text'},
        {'name': '__time_nano__', 'type': 'long'},
        {'name': '__value__', 'type': 'double'},
    ]
    client = LogClient(endpoint, access_key_id, access_key)
    create_metric_store(client, project_name, logstore_name, substore_name, ttl, keys)

    res = client.list_substore(project_name, logstore_name)
    res.log_print()
    res = client.get_substore(project_name, logstore_name, substore_name)
    res.log_print()

    # res = client.update_substore(project_name, logstore_name, substore_name, ttl=ttl,
    #                              keys=keys, sorted_key_count=2, time_index=2)
    # res.log_print()

    res = client.get_substore_ttl(project_name, logstore_name)
    res.log_print()
    res = client.update_substore_ttl(project_name, logstore_name, 60)
    res.log_print()
    res = client.get_substore_ttl(project_name, logstore_name)
    res.log_print()

    res = client.delete_substore(project_name, logstore_name, substore_name)
    res.log_print()
    res = client.delete_logstore(project_name, logstore_name)
    res.log_print()


if __name__ == '__main__':
    main()
