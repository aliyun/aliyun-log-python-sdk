import time
from aliyun.log.compress import Compressor, CompressType
from aliyun.log import LogClient, PutLogsRequest, LogItem, LogGroup


def test_lz4():
    text = b'sadsadsa189634o2??ASBKHD'
    compressed = Compressor.compress(text, CompressType.LZ4)
    raw_size = len(text)
    uncompressed = Compressor.decompress(
        compressed, raw_size, CompressType.LZ4)

    assert text == uncompressed, "The decompressed data does not match the original"


# setup
project = ''
logstore = ''
access_key_id = ''
access_key_secret = ''
endpoint = ''
client = LogClient(endpoint, access_key_id, access_key_secret)


def test_pull_logs():
    shards = client.list_shards(project, logstore).get_shards_info()
    shard = shards[0]['shardID']
    cursor = client.get_cursor(project, logstore, shard, 'begin').get_cursor()
    resp = client.pull_logs(project, logstore, shard, cursor, compress=None)
    resp.log_print()
    client.pull_logs(project, logstore, shard, cursor, compress=True)
    client.pull_logs(project, logstore, shard, cursor, compress=False)


def test_put_log_raw():
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


def test_put_logs():
    client.put_logs(PutLogsRequest(project, logstore, 'test',
                    '', [LogItem(contents=[('ghello', 'test')])], compress=True))
    client.put_logs(PutLogsRequest(project, logstore, 'test',
                    '', [LogItem(contents=[('ghello', 'test')])], compress=None))
    client.put_logs(PutLogsRequest(project, logstore, 'test',
                    '', [LogItem(contents=[('ghello', 'test')])], compress=False))


def test_get_log():
    client._get_logs_v2_enabled = True
    from_time = int(time.time()) - 100
    to_time = int(time.time())
    resp = client.get_log(project, logstore, from_time, to_time, query='*')
    resp.log_print()

    client._get_logs_v2_enabled = False
    resp = client.get_log(project, logstore, from_time, to_time, query='*')
    resp.log_print()
