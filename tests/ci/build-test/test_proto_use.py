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
    str = l.SerializeToString()
    
    d = LogGroupList()
    d.ParseFromString(str)
    assert d.LogGroups[0].Topic == "test"
    assert d.LogGroups[0].Logs[0].Time == 1721908188
    assert d.LogGroups[0].Logs[0].Time_ns == 1
    assert d.LogGroups[0].Logs[0].Contents[0].Key == "a"
    assert d.LogGroups[0].Logs[0].Contents[0].Value == "b"
    
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
    str = l.SerializeToString()
    
    d = LogGroupListRaw()
    d.ParseFromString(str)
    assert d.LogGroups[0].Topic == "test"
    assert d.LogGroups[0].Logs[0].Time == 1721908188
    assert d.LogGroups[0].Logs[0].Time_ns == 1
    assert d.LogGroups[0].Logs[0].Contents[0].Key == "a"
    assert d.LogGroups[0].Logs[0].Contents[0].Value == b"b"
    
    

    