"""End-to-end tests for compress paths against a real SLS endpoint.

The pure ``test_lz4`` round-trip lives in :mod:`tests.unit.test_compress`.
"""
import time

import pytest

from aliyun.log import LogClient, PutLogsRequest, LogItem, LogGroup

from tests._helpers.env import require_sls_env

pytestmark = pytest.mark.e2e


@pytest.fixture
def client_env():
    env = require_sls_env()
    client = LogClient(env["endpoint"], env["access_key_id"], env["access_key_secret"])
    return client, env["project"], env["logstore"]


def test_pull_logs(client_env):
    client, project, logstore = client_env
    shards = client.list_shards(project, logstore).get_shards_info()
    shard = shards[0]['shardID']
    cursor = client.get_cursor(project, logstore, shard, 'begin').get_cursor()
    resp = client.pull_logs(project, logstore, shard, cursor, compress=None)
    resp.log_print()
    client.pull_logs(project, logstore, shard, cursor, compress=True)
    client.pull_logs(project, logstore, shard, cursor, compress=False)


def test_put_log_raw(client_env):
    client, project, logstore = client_env
    log_group = LogGroup()
    log = log_group.Logs.add()
    log.Time = int(time.time())
    for i in range(10):
        content = log.Contents.add()
        content.Key = client._get_unicode('test' + str(i))
        content.Value = client._get_binary('test' + str(i))
    client.put_log_raw(project, logstore, log_group, True)
    client.put_log_raw(project, logstore, log_group, False)
    client.put_log_raw(project, logstore, log_group, None)


def test_put_logs(client_env):
    client, project, logstore = client_env
    client.put_logs(PutLogsRequest(project, logstore, 'test',
                    '', [LogItem(contents=[('ghello', 'test')])], compress=True))
    client.put_logs(PutLogsRequest(project, logstore, 'test',
                    '', [LogItem(contents=[('ghello', 'test')])], compress=None))
    client.put_logs(PutLogsRequest(project, logstore, 'test',
                    '', [LogItem(contents=[('ghello', 'test')])], compress=False))


def test_get_log(client_env):
    client, project, logstore = client_env
    client._get_logs_v2_enabled = True
    from_time = int(time.time()) - 100
    to_time = int(time.time())
    resp = client.get_log(project, logstore, from_time, to_time, query='*')
    resp.log_print()

    client._get_logs_v2_enabled = False
    resp = client.get_log(project, logstore, from_time, to_time, query='*')
    resp.log_print()
