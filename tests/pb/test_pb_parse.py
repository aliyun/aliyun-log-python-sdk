import os
import unittest

import six
import aliyun_log_pb as slspb


class TestParsePB(unittest.TestCase):

    def _gen_varint(self, value):
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

    def test_slspb_parsepb_with_binary_content(self):
        file_dir=os.path.split(os.path.realpath(__file__))[0]
        sample_file = open(file_dir + '/sample_read_with_binary','rb')

        r = sample_file.read()
        sample_file.close()

        (logs, loggroup_cnt) = slspb.parse_pb([r, 1])
        self.assertEqual(1, loggroup_cnt)
        self.assertEqual(1, len(logs))
        self.assertEqual(b'abc\xa0', logs[0]['b'])

    def test_slspb_tns_part_value_invalid(self):
        # if tns_part is invalid, it should be ignored
        bad_tss = [-2147483649,-1,4813906245,1000000000]
        for tns_part in bad_tss:
            datas = [
                0, # do not compress
                '', # topic
                '127.0.0.1', # source
                [("tag1", "mytag1")], # tag
                [1597317524], # log time
                [tns_part], # log time_nano_part
                [[("mytest",u'Hello World!')]] # log key value
            ]

            (pb_str, raw_size, skip_cnt, warnings) = slspb.write_pb(datas)
            loggroup_list_pb_str = bytes.fromhex('0a') + self._gen_varint(len(pb_str)) + pb_str
            (ret, _) = slspb.parse_pb([loggroup_list_pb_str,1])
            self.assertEqual(
                [{'__topic__': '',
                  '__source__': '127.0.0.1',
                  '__time__': '1597317524',
                  'mytest': 'Hello World!',
                  '__tag__:tag1': 'mytag1'}],ret)
        datas = [
            0, # do not compress
            '', # topic
            '127.0.0.1', # source
            [("tag1", "mytag1")], # tag
            [1597317524,1597317525,1597317526,1597317527,1597317528], # log time
            [-2147483649,-1,4813906245,1000000000,123456789], # log time_nano_part
            [[("mytest",u'Hello World!')],[("mytest",u'Hello World!')],[("mytest",u'Hello World!')],[("mytest",u'Hello World!')],[("mytest",u'Hello World!')]] # log key value
        ]
        (pb_str, raw_size, skip_cnt, warnings) = slspb.write_pb(datas)
        loggroup_list_pb_str = bytes.fromhex('0a') + self._gen_varint(len(pb_str)) + pb_str
        (ret, _) = slspb.parse_pb([loggroup_list_pb_str,1])
        self.assertEqual([{'__topic__': '', '__source__': '127.0.0.1', '__time__': '1597317524', 'mytest': 'Hello World!', '__tag__:tag1': 'mytag1'},
                           {'__topic__': '', '__source__': '127.0.0.1', '__time__': '1597317525', 'mytest': 'Hello World!', '__tag__:tag1': 'mytag1'},
                             {'__topic__': '', '__source__': '127.0.0.1', '__time__': '1597317526', 'mytest': 'Hello World!', '__tag__:tag1': 'mytag1'},
                               {'__topic__': '', '__source__': '127.0.0.1', '__time__': '1597317527', 'mytest': 'Hello World!', '__tag__:tag1': 'mytag1'}, 
                               {'__topic__': '', '__source__': '127.0.0.1', '__time__': '1597317528', '__time_ns_part__': '123456789', 'mytest': 'Hello World!', '__tag__:tag1': 'mytag1'}
                               ],ret)

    def test_slspb_parse_time_nano_part(self):
        pb_str = b'\n\x13\x08\x94\xc3\xd4\xf9\x05%\x93s\xc9)\x12\x06\n\x01a\x12\x01b\x1a\x00"\t128.9.9.12\x06\n\x014\x12\x0152\x06\n\x016\x12\x017'
        loggroup_list_pb_str = bytes.fromhex('0a') + self._gen_varint(len(pb_str)) + pb_str
        (ret, _) = slspb.parse_pb([loggroup_list_pb_str,1])
        self.assertEqual(
            [{'__topic__': '',
              '__source__': '128.9.9.1',
              '__time__': '1597317524',
              '__time_ns_part__':'701068179',
              'a': 'b',
              '__tag__:4': '5',
              '__tag__:6': '7'}],ret)

        pb_str =b'\n\x13\x08\x94\xc3\xd4\xf9\x05%\x93s\xc9)\x12\x06\n\x01a\x12\x01b\n\x13\x08\x93\xfb\x90\xab\x06%\x94W\x9a#\x12\x06\n\x01b\x12\x01a\x1a\x00"\t128.9.9.12\x06\n\x014\x12\x0152\x06\n\x016\x12\x017'
        loggroup_list_pb_str = bytes.fromhex('0a') + self._gen_varint(len(pb_str)) + pb_str
        (ret, _) = slspb.parse_pb([loggroup_list_pb_str,1])
        self.assertEqual(
            [{'__topic__': '',
              '__source__': '128.9.9.1',
              '__time__': '1597317524',
              '__time_ns_part__': '701068179',
              'a': 'b',
              '__tag__:4': '5',
              '__tag__:6': '7'},
              {'__topic__': '',
              '__source__': '128.9.9.1',
              '__time__': '1701068179',
              '__time_ns_part__': '597317524',
              'b': 'a',
              '__tag__:4': '5',
              '__tag__:6': '7'}],ret)

    def test_slspb_parse_rawpb_compare(self):
        pb_str = b'\n\x0e\x08\x94\xc3\xd4\xf9\x05\x12\x06\n\x01a\x12\x01b\x1a\x00"\t128.9.9.12\x06\n\x014\x12\x0152\x06\n\x016\x12\x017'

        loggroup_list_pb_str = bytes.fromhex('0a') + self._gen_varint(len(pb_str)) + pb_str
        (ret, _) = slspb.parse_pb([loggroup_list_pb_str,1])
        self.assertEqual(
            [{'__time__': '1597317524',
              '__topic__': '',
              '__source__': '128.9.9.1',
              '__tag__:4': '5',
              '__tag__:6': '7',
              'a': 'b'}], ret)

        pb_str = b'\n\x0e\x08\x94\xc3\xd4\xf9\x05\x12\x06\n\x01a\x12\x01b\x1a\x00"\x0b110.30.1.322\x06\n\x014\x12\x0152\x06\n\x016\x12\x017'
        loggroup_list_pb_str = bytes.fromhex('0a') + self._gen_varint(len(pb_str)) + pb_str
        (ret, _) = slspb.parse_pb([loggroup_list_pb_str,1])
        self.assertEqual(
            [{'__time__': '1597317524',
              '__topic__': '',
              '__source__': '110.30.1.32',
              '__tag__:4': '5',
              '__tag__:6': '7',
              'a': 'b'}], ret)

        pb_str = b'\n\x0e\x08\x94\xc3\xd4\xf9\x05\x12\x06\n\x01a\x12\x01b\x1a\x05hello"\t127.0.0.12\x06\n\x014\x12\x0152\x06\n\x016\x12\x017'

        loggroup_list_pb_str = bytes.fromhex('0a') + self._gen_varint(len(pb_str)) + pb_str
        (ret, _) = slspb.parse_pb([loggroup_list_pb_str,1])
        self.assertEqual(
            [{'__time__': '1597317524',
              '__topic__': 'hello',
              '__source__': '127.0.0.1',
              '__tag__:4': '5',
              '__tag__:6': '7',
              'a': 'b'}], ret)

        pb_str = b'\n\x19\x08\x94\xc3\xd4\xf9\x05\x12\x06\n\x01a\x12\x01b\x12\t\n\x0222\x12\x03abc\x1a\x05hello"\t127.0.0.12\x06\n\x014\x12\x0152\x06\n\x016\x12\x017'
        loggroup_list_pb_str = bytes.fromhex('0a') + self._gen_varint(len(pb_str)) + pb_str
        (ret, _) = slspb.parse_pb([loggroup_list_pb_str,1])
        self.assertEqual(
            [{'__time__': '1597317524',
              '__topic__': 'hello',
              '__source__': '127.0.0.1',
              '__tag__:4': '5',
              '__tag__:6': '7',
              'a': 'b',
              '22': 'abc'}], ret)

        pb_str = b'\n!\x08\x94\xc3\xd4\xf9\x05\x12\x06\n\x01a\x12\x01b\x12\t\n\x0222\x12\x03abc\x12\x06\n\x01c\x12\x01d\x1a\x05hello"\t127.0.0.12\x06\n\x014\x12\x0152\x06\n\x016\x12\x017'
        loggroup_list_pb_str = bytes.fromhex('0a') + self._gen_varint(len(pb_str)) + pb_str
        (ret, _) = slspb.parse_pb([loggroup_list_pb_str,1])
        self.assertEqual(
            [{'__time__': '1597317524',
              '__topic__': 'hello',
              '__source__': '127.0.0.1',
              '__tag__:4': '5',
              '__tag__:6': '7',
              'a': 'b',
              '22': 'abc',
              'c': 'd'}], ret)

        pb_str = b'\n0\x08\x94\xc3\xd4\xf9\x05\x12\x06\n\x01a\x12\x01b\x12\t\n\x0222\x12\x03abc\x12\x06\n\x01c\x12\x01d\x12\r\n\x01x\x12\x08abc\xa0abfa\x1a\x05hello"\t127.0.0.12\x06\n\x014\x12\x0152\x06\n\x016\x12\x017'
        loggroup_list_pb_str = bytes.fromhex('0a') + self._gen_varint(len(pb_str)) + pb_str
        (ret, _) = slspb.parse_pb([loggroup_list_pb_str,1])
        self.assertEqual(
            [{'__time__': '1597317524',
              '__topic__': 'hello',
              '__source__': '127.0.0.1',
              '__tag__:4': '5',
              '__tag__:6': '7',
              'a': 'b',
              '22': 'abc',
              'c': 'd',
              'x': b'abc\xa0abfa'}], ret)

        pb_str = b'\n0\x08\x94\xc3\xd4\xf9\x05\x12\x06\n\x01a\x12\x01b\x12\t\n\x0222\x12\x03abc\x12\x06\n\x01c\x12\x01d\x12\r\n\x01x\x12\x08abc\xa0abfa\x1a\x05hello"\t127.0.0.1'
        loggroup_list_pb_str = bytes.fromhex('0a') + self._gen_varint(len(pb_str)) + pb_str
        (ret, _) = slspb.parse_pb([loggroup_list_pb_str,1])
        self.assertEqual(
            [{'__time__': '1597317524',
              '__topic__': 'hello',
              '__source__': '127.0.0.1',
              'a': 'b',
              '22': 'abc',
              'c': 'd',
              'x': b'abc\xa0abfa'}], ret)

        pb_str = b'\n0\x08\x94\xc3\xd4\xf9\x05\x12\x06\n\x01a\x12\x01b\x12\t\n\x0222\x12\x03abc\x12\x06\n\x01c\x12\x01d\x12\r\n\x01x\x12\x08abc\xa0abfa\x1a\x05hello"\t127.0.0.12\x0c\n\x05tag_k\x12\x03txx'
        loggroup_list_pb_str = bytes.fromhex('0a') + self._gen_varint(len(pb_str)) + pb_str
        (ret, _) = slspb.parse_pb([loggroup_list_pb_str,1])
        self.assertEqual(
            [{'__time__': '1597317524',
              '__topic__': 'hello',
              '__source__': '127.0.0.1',
              '__tag__:tag_k': 'txx',
              'a': 'b',
              '22': 'abc',
              'c': 'd',
              'x': b'abc\xa0abfa'}], ret)

    def test_slspb_parsepb_simple(self):
        file_dir=os.path.split(os.path.realpath(__file__))[0]
        sample_file = open(file_dir + '/sample_read','rb')
        r = sample_file.read()
        sample_file.close()

        (logs, loggroup_cnt) = slspb.parse_pb([r, 1])
        self.assertEqual(1, loggroup_cnt)
        self.assertEqual(100, len(logs))

    def test_slspb_parsepb_arg_check(self):
        with self.assertRaises(Exception) as context:
            slspb.parse_pb("a")
        self.assertTrue('parse_pb except a list' in str(context.exception))

        with self.assertRaises(Exception) as context:
            slspb.parse_pb([])
        self.assertTrue('parse_pb except a list' in str(context.exception))

        with self.assertRaises(Exception) as context:
            slspb.parse_pb([1,3])
        # print(str(context.exception))
        self.assertTrue('except bytes at first element in list' in str(context.exception))

        with self.assertRaises(Exception) as context:
            slspb.parse_pb([b'123',"2"])
        # print(str(context.exception))
        self.assertTrue('except int at second element in list' in str(context.exception))

        with self.assertRaises(Exception) as context:
            slspb.parse_pb([b'123',3])
        # print(str(context.exception))
        self.assertTrue('except second element value 0 or 1' in str(context.exception))

        (ret, cnt) = slspb.parse_pb([b'123',1])
        self.assertEqual([], ret)

        (ret, cnt) = slspb.parse_pb([b'',1])
        self.assertEqual([], ret)


if __name__ == '__main__':
    unittest.main()