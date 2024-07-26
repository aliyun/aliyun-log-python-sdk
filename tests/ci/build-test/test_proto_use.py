# -*- encoding: utf-8 -*-
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
    tag = log.LogTags.add()
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
    assert d.LogGroups[0].Logs[0].LogTags[0].Key == u"c中文"
    assert d.LogGroups[0].Logs[0].LogTags[0].Value == u"d中文"


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
    tag = log.LogTags.add()
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
    assert d.LogGroups[0].Logs[0].LogTags[0].Key == u"c中文"
    assert d.LogGroups[0].Logs[0].LogTags[0].Value == u"d中文"
