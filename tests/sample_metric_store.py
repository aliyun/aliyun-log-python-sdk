# encoding: utf-8
from __future__ import print_function

import time

from aliyun.log import *


def main():
    endpoint = 'cn-hangzhou.log.aliyuncs.com'
    access_key_id = ''
    access_key = ''
    project_name = 'test-metricstore-' + str(int(time.time()))
    logstore_name = 'test_metricstore'

    client = LogClient(endpoint, access_key_id, access_key)

    res = client.create_project(project_name, '')
    res.log_print()
    time.sleep(60)

    res = client.create_metric_store(project_name, logstore_name, 18)
    res.log_print()

    res = client.get_metric_store(project_name, logstore_name)
    assert res.ttl == 18
    assert res.telemetry_type == 'Metrics'

    res = client.update_metric_store(project_name, logstore_name, 19)
    res.log_print()

    res = client.get_metric_store(project_name, logstore_name)
    assert res.ttl == 19
    assert res.telemetry_type == 'Metrics'

    res = client.delete_metric_store(project_name, logstore_name)
    res.log_print()

    res = client.delete_project(project_name)
    res.log_print()


if __name__ == '__main__':
    main()
