import sys
sys.path.append('/home/admin/etl-client-1')
import unittest
import sls_logs_pb2
import slspb
import six  
def _gen_varint(value):
    pieces = []
    bits = value & 0x7f
    value >>= 7

    while value:
        pieces.append(six.int2byte(0x80|bits))
        bits = value & 0x7f
        value >>= 7
    pieces.append(six.int2byte(bits))
    out = pieces[0]

    for i in range(1,len(pieces)):
        out += pieces[i]
    return out

def encode_log(log):
    # 创建Log对象并设置字段值
    log_msg = sls_logs_pb2.Log()
    log_msg.Time = log["Time"]

    for content in log["Contents"]:
        content_msg = log_msg.Content()
        content_msg.Key = content["Key"]
        content_msg.Value = content["Value"]
        log_msg.Contents.append(content_msg)

    log_msg.Reserved.extend(log["Reserved"])
    if "Time_ns" in log:
        log_msg.Time_ns = log["Time_ns"]

    # 将Log对象序列化为二进制数据
    data = log_msg.SerializeToString()
    return data

def decode_log(data):
    # 将二进制数据反序列化为Log对象
    log_msg = sls_logs_pb2.Log()
    log_msg.ParseFromString(data)
    print(log_msg)
    # 从Log对象中获取字段值并构建字典
    log = {
        "Time": log_msg.Time,
        "Contents": [],
    }
    if log_msg.HasField("Time_ns"):
        log["Time_ns"] = log_msg.Time_ns

    for content_msg in log_msg.Contents:
        content = {
            "Key": content_msg.Key,
            "Value": content_msg.Value
        }
        log["Contents"].append(content)

    return log


def encode_log_group(log_group):
    # 创建LogGroup对象并设置字段值
    log_group_msg = sls_logs_pb2.LogGroup()

    for log in log_group["Logs"]:
        log_msg = log_group_msg.Logs.add()
        log_msg.Time = log["Time"]

        for content in log["Contents"]:
            content_msg = log_msg.Contents.add()
            for k,v in content.items():
                content_msg.Key = k
                content_msg.Value = v
        if "Time_ns" in log:
            log_msg.Time_ns = log["Time_ns"]

    log_group_msg.Topic = log_group["Topic"]
    log_group_msg.Source = log_group["Source"]
    for log_tag in log_group["LogTags"]:
        log_tag_msg = log_group_msg.LogTags.add()
        for k,v in log_tag.items():
            log_tag_msg.Key = k
            log_tag_msg.Value = v

    # 将LogGroup对象序列化为二进制数据
    data = log_group_msg.SerializeToString()
    return data

def encode_log_group2(log_group):
    # 创建LogGroup对象并设置字段值
    log_group_msg = sls_logs_pb2.LogGroup()
    for log in log_group["Logs"]:
        log_msg = log_group_msg.Logs.add()
        log_msg.Time = log["Time"]
        for content in log["Contents"]:
            content_msg = log_msg.Contents.add()
            for k,v in content.items():
                content_msg.Key = k
                content_msg.Value = v
        if "Time_ns" in log:
            log_msg.Time_ns = log["Time_ns"]
        log_msg.Reserved.extend(log["Reserved"])
    log_group_msg.Topic = log_group["Topic"]
    log_group_msg.Source = log_group["Source"]
    for log_tag in log_group["LogTags"]:
        log_tag_msg = log_group_msg.LogTags.add()
        for k,v in log_tag.items():
            log_tag_msg.Key = k
            log_tag_msg.Value = v

    # 将LogGroup对象序列化为二进制数据
    data = log_group_msg.SerializeToString()
    return data

def decode_log_group(data):
    log_group_list_msg = sls_logs_pb2.LogGroupList()
    log_group_list_msg.ParseFromString(data)
    for log_group_msg in log_group_list_msg.LogGroups:
        log_group = []

        for log_msg in log_group_msg.Logs:
            log = {
                "__topic__": log_group_msg.Topic,
                "__source__": log_group_msg.Source,
                "__time__": str(log_msg.Time),
            }
            if log_msg.HasField("Time_ns"):
                log["__time_ns_part__"] = str(log_msg.Time_ns) 

            for content_msg in log_msg.Contents:
                log[content_msg.Key] = content_msg.Value

            for log_tag_msg in log_group_msg.LogTags:
                log["__tag__:" + log_tag_msg.Key] = log_tag_msg.Value
            log_group.append(log)
    return log_group

class TestWritePB(unittest.TestCase):
    # test reserved exist, encode and decode can run successfully
    def test_reserved_base2sls(self):
        log_group = {
            "Logs": [{
            "Time": 1597317524,
            "Time_ns": 12345,
            "Reserved": ["hello", "world"],
            "Contents": [{"a": "b"},{"a1": "b1"}]
        }],
            "Topic": "hello",
            "Source": "128.9.9.1",
            "LogTags": [{"4": "5"},{"6": "7"}]
        }
        encoded_log_group = encode_log_group2(log_group)
        (pb_str, raw_size, skip_cnt, warnings) = slspb.write_pb([0,"hello","128.9.9.1",[("4","5"),("6","7")],[1597317524],[12345],[[("a","b"),("a1","b1")]]])

        encode_base = bytes.fromhex('0a') + _gen_varint(len(encoded_log_group)) + encoded_log_group
        encode_sls = bytes.fromhex('0a') + _gen_varint(len(pb_str)) + pb_str

        base2base =  decode_log_group(encode_base)
        sls2base = decode_log_group(encode_sls)  
        (base2sls, loggroup_cnt) = slspb.parse_pb([encode_base,1])  
        (sls2sls, loggroup_cnt)= slspb.parse_pb([encode_sls,1])
        self.assertEqual(base2base, sls2base)
        self.assertEqual(base2base, base2sls)
        self.assertEqual(sls2base, sls2sls)
        self.assertEqual(base2sls, sls2sls)

    def test_log_slspb_vs_basepb_without_ns(self):
        log_group = {
            "Logs": [{
            "Time": 1597317524,
            "Contents": [{"a": "b"},{"a1": "b1"}]
        }],
            "Topic": "hello",
            "Source": "128.9.9.1",
            "LogTags": [{"4": "5"},{"6": "7"}]
        }
        encoded_log_group = encode_log_group(log_group)
        (pb_str, raw_size, skip_cnt, warnings) = slspb.write_pb([0,"hello","128.9.9.1",[("4","5"),("6","7")],[1597317524],[[("a","b"),("a1","b1")]]])

        encode_base = bytes.fromhex('0a') + _gen_varint(len(encoded_log_group)) + encoded_log_group
        encode_sls = bytes.fromhex('0a') + _gen_varint(len(pb_str)) + pb_str

        base2base =  decode_log_group(encode_base)
        sls2base = decode_log_group(encode_sls)  
        (base2sls, loggroup_cnt) = slspb.parse_pb([encode_base,1])  
        (sls2sls, loggroup_cnt)= slspb.parse_pb([encode_sls,1])

        self.assertEqual(base2base, sls2base)
        self.assertEqual(base2base, base2sls)
        self.assertEqual(sls2base, sls2sls)
        self.assertEqual(base2sls, sls2sls)

    def test_log_slspb_vs_basepb(self):
        log_group = {
            "Logs": [{
            "Time": 1597317524,
            "Time_ns": 12345,
            "Contents": [{"a": "b"},{"a1": "b1"}]
        }],
            "Topic": "hello",
            "Source": "128.9.9.1",
            "LogTags": [{"4": "5"},{"6": "7"}]
        }
        encoded_log_group = encode_log_group(log_group)
        (pb_str, raw_size, skip_cnt, warnings) = slspb.write_pb([0,"hello","128.9.9.1",[("4","5"),("6","7")],[1597317524],[12345],[[("a","b"),("a1","b1")]]])

        encode_base = bytes.fromhex('0a') + _gen_varint(len(encoded_log_group)) + encoded_log_group
        encode_sls = bytes.fromhex('0a') + _gen_varint(len(pb_str)) + pb_str

        base2base =  decode_log_group(encode_base)
        sls2base = decode_log_group(encode_sls)  
        (base2sls, loggroup_cnt) = slspb.parse_pb([encode_base,1])  
        (sls2sls, loggroup_cnt)= slspb.parse_pb([encode_sls,1])

        self.assertEqual(base2base, sls2base)
        self.assertEqual(base2base, base2sls)
        self.assertEqual(sls2base, sls2sls)
        self.assertEqual(base2sls, sls2sls)

    def test_log_slspb_vs_basepb_error(self):
        log_group = {
            "Logs": [{
            "Time": 1597317524,
            "Time_ns": 1234567890,
            "Contents": [{"a": "b"},{"a1": "b1"}]
        }],
            "Topic": "hello",
            "Source": "128.9.9.1",
            "LogTags": [{"4": "5"},{"6": "7"}]
        }
        encoded_log_group = encode_log_group(log_group)
        (pb_str, raw_size, skip_cnt, warnings) = slspb.write_pb([0,"hello","128.9.9.1",[("4","5"),("6","7")],[1597317524],[1234567890],[[("a","b"),("a1","b1")]]])

        encode_base = bytes.fromhex('0a') + _gen_varint(len(encoded_log_group)) + encoded_log_group
        encode_sls = bytes.fromhex('0a') + _gen_varint(len(pb_str)) + pb_str

        base2base =  decode_log_group(encode_base)
        sls2base = decode_log_group(encode_sls)  
        (base2sls, loggroup_cnt) = slspb.parse_pb([encode_base,1])  
        (sls2sls, loggroup_cnt)= slspb.parse_pb([encode_sls,1])
        self.assertEqual([{'__topic__': 'hello', '__source__': '128.9.9.1', '__time__': '1597317524', 'a': 'b', 'a1': 'b1', '__tag__:4': '5', '__tag__:6': '7'}], sls2base)
        self.assertEqual([{'__topic__': 'hello', '__source__': '128.9.9.1', '__time__': '1597317524', 'a': 'b', 'a1': 'b1', '__tag__:4': '5', '__tag__:6': '7'}], base2sls)
        self.assertEqual([{'__topic__': 'hello', '__source__': '128.9.9.1', '__time__': '1597317524', 'a': 'b', 'a1': 'b1', '__tag__:4': '5', '__tag__:6': '7'}], sls2base)
        self.assertEqual([{'__topic__': 'hello', '__source__': '128.9.9.1', '__time__': '1597317524', '__time_ns_part__': '1234567890', 'a': 'b', 'a1': 'b1', '__tag__:4': '5', '__tag__:6': '7'}], base2base)

    def test_logs_slspb_vs_basepb(self):
        log_group = {
            "Logs": [{
            "Time": 1597317524,
            "Time_ns": 12345,
            "Contents": [{"a": "b"},{"a1": "b1"}]
        },
        {
            "Time": 1597317525,
            "Time_ns": 12346,
            "Contents": [{"a": "b"},{"a1": "b1"}]
        }],
            "Topic": "hello",
            "Source": "128.9.9.1",
            "LogTags": [{"4": "5"},{"6": "7"}]
        }
        encoded_log_group = encode_log_group(log_group)
        (pb_str, raw_size, skip_cnt, warnings) = slspb.write_pb([0,"hello","128.9.9.1",[("4","5"),("6","7")],[1597317524,1597317525],[12345,12346],[[("a","b"),("a1","b1")],[("a","b"),("a1","b1")]]])

        encode_base = bytes.fromhex('0a') + _gen_varint(len(encoded_log_group)) + encoded_log_group
        encode_sls = bytes.fromhex('0a') + _gen_varint(len(pb_str)) + pb_str

        base2base =  decode_log_group(encode_base)
        sls2base = decode_log_group(encode_sls)  
        (base2sls, loggroup_cnt) = slspb.parse_pb([encode_base,1])  
        (sls2sls, loggroup_cnt)= slspb.parse_pb([encode_sls,1])

        self.assertEqual(base2base, sls2base)
        self.assertEqual(base2base, base2sls)
        self.assertEqual(sls2base, sls2sls)
        self.assertEqual(base2sls, sls2sls)

if __name__ == '__main__':
    unittest.main()
