# -*- encoding: utf-8 -*-
import sys

import pytest

from aliyun.log.proto import *


def test_pb_encode():
    l = LogGroupList()
    log_group = l.LogGroups.add()
    log_group.Topic = "test"
    log = log_group.Logs.add()
    log.Time = 1721908188
    log.Time_ns = 1
    content = log.Contents.add()
    content.Key = "a"
    content.Value = "b"
    tag = log_group.LogTags.add()
    tag.Key = u"c中文"
    tag.Value = u"d中文"
    str = l.SerializeToString()

    d = LogGroupList()
    d.ParseFromString(str)
    assert d.LogGroups[0].Topic == "test"
    assert d.LogGroups[0].Logs[0].Time == 1721908188
    assert d.LogGroups[0].Logs[0].Time_ns == 1
    assert d.LogGroups[0].Logs[0].Contents[0].Key == "a"
    assert d.LogGroups[0].Logs[0].Contents[0].Value == "b"
    assert d.LogGroups[0].LogTags[0].Key == u"c中文"
    assert d.LogGroups[0].LogTags[0].Value == u"d中文"


@pytest.mark.skipif(sys.version_info[0] == 2, reason="Async SQL requires Python 3")
def test_py3_generated_bindings():
    from google.protobuf import __version__

    from aliyun.log import async_sql_pb2, log_logs_pb2, log_logs_raw_pb2

    package = 'aliyun.log._protobuf.v' + __version__.split('.', 1)[0]
    for module, name in ((log_logs_pb2, 'LogGroupList'),
                         (log_logs_raw_pb2, 'LogGroupListRaw'),
                         (async_sql_pb2, 'AsyncSqlResponsePB')):
        generated = sys.modules[package + '.' + module.__name__.rsplit('.', 1)[1]]
        assert module.DESCRIPTOR is generated.DESCRIPTOR
        assert getattr(module, name) is getattr(generated, name)

    assert log_logs_pb2.LogGroupList is LogGroupList
    assert log_logs_raw_pb2.LogGroupListRaw is LogGroupListRaw

    response = async_sql_pb2.AsyncSqlResponsePB(id='query-id', state='Complete')
    response.meta.keys.append('message')
    response.rows.add().columns.append(u'中文')
    decoded = async_sql_pb2.AsyncSqlResponsePB()
    decoded.ParseFromString(response.SerializeToString())
    assert decoded == response


def test_pb_raw_encode():
    l = LogGroupListRaw()
    log_group = l.LogGroups.add()
    log_group.Topic = "test"
    log = log_group.Logs.add()
    log.Time = 1721908188
    log.Time_ns = 1
    content = log.Contents.add()
    content.Key = "a"
    content.Value = b"b"
    tag = log_group.LogTags.add()
    tag.Key = u"c中文"
    tag.Value = u"d中文"
    str = l.SerializeToString()

    d = LogGroupListRaw()
    d.ParseFromString(str)
    assert d.LogGroups[0].Topic == "test"
    assert d.LogGroups[0].Logs[0].Time == 1721908188
    assert d.LogGroups[0].Logs[0].Time_ns == 1
    assert d.LogGroups[0].Logs[0].Contents[0].Key == "a"
    assert d.LogGroups[0].Logs[0].Contents[0].Value == b"b"
    assert d.LogGroups[0].LogTags[0].Key == u"c中文"
    assert d.LogGroups[0].LogTags[0].Value == u"d中文"
