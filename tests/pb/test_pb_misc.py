import unittest

import six
import time
import aliyun_log_pb as slspb
import random
import pickle
from aliyun.log.util import lz_decompress

from concurrent.futures import ThreadPoolExecutor, as_completed


class TestPBMisc(unittest.TestCase):

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

    def test_slspb_wirte_with_lz4_and_parse(self):
        ts1 = int(time.time())
        datas = [
            1, # do compress
            '中文1', # topic
            '127.0.0.1', # source
            [("tag1", "中文1"),
             ], # tag
            [ts1], # log time
            [[("key1","中文2"),("key2", ""),("key3","contentxxx")]]# log key value
        ]

        (pb_str, raw_size, skip_cnt, warnings)  = slspb.write_pb(datas)
        self.assertEqual(0, len(warnings))
        self.assertEqual(0, skip_cnt)

        pb_str = lz_decompress(raw_size, pb_str)

        loggroup_list_pb_str = bytes.fromhex('0a') + self._gen_varint(len(pb_str)) + pb_str
        (logs, loggroup_cnt) = slspb.parse_pb([loggroup_list_pb_str,1])
        self.assertEqual(1, loggroup_cnt)
        self.assertEqual(1, len(logs))
        self.assertEqual('127.0.0.1', logs[0]['__source__'])
        self.assertEqual('中文1', logs[0]['__topic__'])
        self.assertEqual('中文2', logs[0]['key1'])
        self.assertEqual('', logs[0]['key2'])
        self.assertEqual('contentxxx', logs[0]['key3'])


    def test_slspb_write_and_parse(self):
        ts1 = int(time.time())
        time.sleep(2)
        ts2 = int(time.time())
        long_str_list = []
        for i in range(1,20000):
            long_str_list.append("abcxxx")
        long_str1 = ''.join(long_str_list)
        long_str2 = 'x'.join(long_str_list)
        datas = [
            0, # do not compress
            None, # topic
            '127.0.0.1', # source
            [("tag1", "mytag1"),
             ("tag2",b'\xa0axxxb'),
             ("tag3",None),
             (long_str1, long_str1),
             ], # tag
            [ts1, ts2, 0], # log time
            [[("mytest","content")],
             [("binary_k",b'\xa0ab'),
                ("val1","1"),("val2","None"),("val3","NULL"),
                ("val4","3.14"),("val5",'{"a":1}'),
                ("val6",b'{"msg":"There iaows\\n?EDAS Console - A -\xa0Instance '),
                ("val7",long_str2),
                ("val8","中文"),
                ("val9","にほんご "),
                ("val10","π\0排球の@@"),
                ("val11",b'A\xc3\x84B\xc3\xa8C'),
                (long_str2,long_str2),
            ],
            [("hh", "hoho"),("empty_val", None)]] # log key value
        ]

        (pb_str, raw_size, skip_cnt, warnings) = slspb.write_pb(datas)
        self.assertEqual(len(warnings), 2)
        self.assertEqual(skip_cnt, 0)

        self.assertTrue("tag value is too long, limit to 255" in warnings[0])
        self.assertTrue("ts is value invalid, will be reset to system time" in warnings[1])

        loggroup_list_pb_str = bytes.fromhex('0a') + self._gen_varint(len(pb_str)) + pb_str

        (logs, loggroup_cnt) = slspb.parse_pb([loggroup_list_pb_str,1])
        self.assertEqual(1, loggroup_cnt)
        self.assertEqual(3, len(logs))
        self.assertEqual('content', logs[0]['mytest'])
        self.assertEqual(str(ts1), logs[0]['__time__'])
        self.assertLessEqual(ts2, int(logs[2]['__time__']))

        self.assertEqual(b'\xa0axxxb', logs[1]['__tag__:tag2'])
        self.assertEqual('', logs[1]['__tag__:tag3'])
        self.assertEqual('', logs[1]['__topic__'])
        self.assertEqual('', logs[2]['empty_val'])
        self.assertEqual(b'\xa0ab', logs[1]['binary_k'])
        self.assertEqual(str(ts2), logs[1]['__time__'])
        self.assertEqual("1", logs[1]['val1'])
        self.assertEqual("None", logs[1]['val2'])
        self.assertEqual("NULL", logs[1]['val3'])
        self.assertEqual("3.14", logs[1]['val4'])
        self.assertEqual('{"a":1}', logs[1]['val5'])
        self.assertEqual(b'{"msg":"There iaows\\n?EDAS Console - A -\xa0Instance ',logs[1]['val6'])
        self.assertEqual(long_str2, logs[1]['val7'])
        self.assertEqual(long_str2, logs[1][long_str2])
        # self.assertEqual(long_str1, logs[1]['__tag__:'+long_str1[:256]])
        self.assertEqual("中文", logs[1]['val8'])
        self.assertEqual("にほんご ", logs[1]['val9'])
        self.assertEqual("π\0排球の@@", logs[1]['val10'])
        self.assertEqual(b'A\xc3\x84B\xc3\xa8C'.decode("utf-8"), logs[1]['val11'])

        datas = [
            0, # do not compress
            '中文1', # topic
            '127.0.0.1', # source
            [("tag1", "中文1"),
             (long_str1, long_str1),
             ], # tag
            [ts1], # log time
            [[("key1","中文2"),("key2", ""),("key3","contentxxx")]]# log key value
        ]

        (pb_str, raw_size, skip_cnt, warnings)  = slspb.write_pb(datas)
        self.assertEqual(1, len(warnings))
        self.assertEqual(0, skip_cnt)

        loggroup_list_pb_str = bytes.fromhex('0a') + self._gen_varint(len(pb_str)) + pb_str

        (logs, loggroup_cnt) = slspb.parse_pb([loggroup_list_pb_str,1])
        self.assertEqual(1, loggroup_cnt)
        self.assertEqual(1, len(logs))
        self.assertEqual('127.0.0.1', logs[0]['__source__'])
        self.assertEqual('中文1', logs[0]['__topic__'])
        self.assertEqual('中文2', logs[0]['key1'])
        self.assertEqual('', logs[0]['key2'])
        self.assertEqual('contentxxx', logs[0]['key3'])

    def test_slspb_large_write_and_parse(self):
        datas = [
            0, # do not compress
            '', # topic
            '127.0.0.1', # source
            [("tag1", "mytag1"),("tag2","mytag2")], # tag
            [], # log time
            [] # log key value
        ]
        bigsize = 500000 # about 27MB
        for i in range(0,bigsize):
            datas[4].append(int(time.time()))
            datas[5].append([("mytest","contentxxxxxxxxxxxxxxx"),
                             ("idx", str(i)),
                             ("binary_val",b'abc\xa0abfadsfas\ndfas')])

        (pb_str, raw_size, skip_cnt, warnings) = slspb.write_pb(datas)
        self.assertEqual(0, len(warnings))
        self.assertEqual(0, skip_cnt)

        groups = 20
        loggroup_list_pb_str = None
        for i in range(0, groups):
            if loggroup_list_pb_str == None:
                loggroup_list_pb_str = bytes.fromhex('0a') + self._gen_varint(len(pb_str)) + pb_str
            else:
                loggroup_list_pb_str += bytes.fromhex('0a') + self._gen_varint(len(pb_str)) + pb_str

        # loggroup_list_pb_str about 27M * 20 = 540MB
        # open('/tmp/pxp','wb').write(loggroup_list_pb_str)
        (logs, loggroup_cnt) = slspb.parse_pb([loggroup_list_pb_str,1])
        self.assertEqual(groups, loggroup_cnt)
        self.assertEqual(bigsize*groups, len(logs))
        idx = random.randint(1,bigsize-1)
        self.assertEqual('contentxxxxxxxxxxxxxxx', logs[idx]['mytest'])
        self.assertEqual(b'abc\xa0abfadsfas\ndfas', logs[idx]['binary_val'])
        self.assertEqual(str(idx), logs[idx]['idx'])

    def test_slspb_large_write_lz4_and_parse(self):
        datas = [
            1, # do not compress
            '', # topic
            '127.0.0.1', # source
            [("tag1", "mytag1"),("tag2","mytag2")], # tag
            [], # log time
            [] # log key value
        ]
        bigsize = 500000 # about 27MB
        for i in range(0,bigsize):
            datas[4].append(int(time.time()))
            datas[5].append([("mytest","contentxxxxxxxxxxxxxxx"),
                             ("idx", str(i)),
                             ("binary_val",b'abc\xa0abfadsfas\ndfas')])

        (pb_str, raw_size, skip_cnt, warnings)  = slspb.write_pb(datas)
        self.assertEqual(0, len(warnings))
        self.assertEqual(0, skip_cnt)

        pb_str = lz_decompress(raw_size, pb_str)

        groups = 20
        loggroup_list_pb_str = None
        for i in range(0, groups):
            if loggroup_list_pb_str == None:
                loggroup_list_pb_str = bytes.fromhex('0a') + self._gen_varint(len(pb_str)) + pb_str
            else:
                loggroup_list_pb_str += bytes.fromhex('0a') + self._gen_varint(len(pb_str)) + pb_str

        # loggroup_list_pb_str about 27M * 20 = 540MB
        # open('/tmp/pxp','wb').write(loggroup_list_pb_str)
        (logs, loggroup_cnt) = slspb.parse_pb([loggroup_list_pb_str,1])
        self.assertEqual(groups, loggroup_cnt)
        self.assertEqual(bigsize*groups, len(logs))
        idx = random.randint(1,bigsize-1)
        self.assertEqual('contentxxxxxxxxxxxxxxx', logs[idx]['mytest'])
        self.assertEqual(b'abc\xa0abfadsfas\ndfas', logs[idx]['binary_val'])
        self.assertEqual(str(idx), logs[idx]['idx'])

    def test_slspb_binary_write_and_parse(self):
        datas = [
            0, # do not compress
            '', # topic
            '127.0.0.1', # source
            [("tag1", "mytag1"),("tag2","mytag2")], # tag
            [int(time.time())], # log time
            [[("mytest",b'abc\xa0')]] # log key value
        ]

        (pb_str, raw_size, skip_cnt, warnings)  = slspb.write_pb(datas)
        self.assertEqual(0, len(warnings))
        self.assertEqual(0, skip_cnt)

        loggroup_list_pb_str = bytes.fromhex('0a') + self._gen_varint(len(pb_str)) + pb_str

        (logs, loggroup_cnt) = slspb.parse_pb([loggroup_list_pb_str,1])
        self.assertEqual(1, loggroup_cnt)
        self.assertEqual(1, len(logs))
        self.assertEqual(b'abc\xa0',logs[0]['mytest'])

        datas = [
            0, # do not compress
            '', # topic
            '127.0.0.1', # source
            [("tag1", "mytag1"),("tag2","mytag2")], # tag
            [int(time.time()),int(time.time())], # log time
            [[("mytest",b'abc\xa0'),("mytest2","aaa")],[("aaa","bbb")]] # log key value
        ]

        (pb_str, raw_size, skip_cnt, warnings) = slspb.write_pb(datas)
        self.assertEqual(0, skip_cnt)
        self.assertEqual(0, len(warnings))

        loggroup_list_pb_str = bytes.fromhex('0a') + self._gen_varint(len(pb_str)) + pb_str

        (logs, loggroup_cnt) = slspb.parse_pb([loggroup_list_pb_str,1])
        self.assertEqual(1, loggroup_cnt)
        self.assertEqual(2, len(logs))
        self.assertEqual(b'abc\xa0',logs[0]['mytest'])
        self.assertEqual("bbb",logs[1]['aaa'])

        datas = [
            0, # do not compress
            b'2409:8809:18:e6ca:5a38:3c27:587f:66e3\x98', # topic
            b'2409:8809:18:e6ca:5a38:3c27:587f:66e3\x98', # source
            [("tag1", "mytag1"),("tag2","mytag2")], # tag
            [int(time.time()),int(time.time())], # log time
            [[("mytest",b'abc\xa0'),("mytest2","aaa")],[("aaa","bbb")]] # log key value
        ]

        (pb_str, raw_size, skip_cnt, warnings) = slspb.write_pb(datas)
        self.assertEqual(0, skip_cnt)
        self.assertEqual(0, len(warnings))

        loggroup_list_pb_str = bytes.fromhex('0a') + self._gen_varint(len(pb_str)) + pb_str

        (logs, loggroup_cnt) = slspb.parse_pb([loggroup_list_pb_str, 1])
        self.assertEqual(1, loggroup_cnt)
        self.assertEqual(2, len(logs))
        self.assertEqual(b'abc\xa0',logs[0]['mytest'])
        self.assertEqual("bbb",logs[1]['aaa'])
        self.assertEqual(b'2409:8809:18:e6ca:5a38:3c27:587f:66e3\x98', logs[1]['__topic__'])
        self.assertEqual(b'2409:8809:18:e6ca:5a38:3c27:587f:66e3\x98', logs[1]['__source__'])


    def test_slspb_multithreading(self):

        # pb write
        def _pb_write(datas):
            return slspb.write_pb(datas)

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            for i in range(0,100):
                datas = [
                    0, # do not compress
                    '', # topic
                    '127.0.0.1', # source
                    [("tag1", "mytag1"),("tag2","mytag2")], # tag
                    [int(time.time())], # log time
                    [[("mytest","content")]] # log key value
                ]
                futures.append(executor.submit(_pb_write, datas))
            success_cnt = 0
            for f in as_completed(futures):
                (pb_str, raw_size, skip_cnt, warnings) = f.result()
                self.assertEqual(72, len(pb_str))
                self.assertEqual(0, skip_cnt)
                self.assertEqual(0, len(warnings))
                success_cnt += 1
            self.assertEqual(success_cnt, 100)

        # pb parse
        pb_str_list = bytes.fromhex('0a') + self._gen_varint(len(pb_str)) + pb_str
        def _pb_parse(data):
            return slspb.parse_pb([data, 1])

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            for i in range(0,100):
                data = pickle.loads(pickle.dumps(pb_str_list))
                futures.append(executor.submit(_pb_parse, data))
            success_cnt = 0
            for f in as_completed(futures):
                (logs, loggroup_cnt) = f.result()
                self.assertEqual(1, loggroup_cnt)
                self.assertEqual(1, len(logs))
                self.assertEqual(logs[0]['mytest'],'content')
                success_cnt += 1
            self.assertEqual(success_cnt, 100)

        # pb write and parse
        def _pb_parse_write(datas):
            (pb_str, raw_size, skip_cnt, warnings)  = slspb.write_pb(datas)
            pb_str_list = bytes.fromhex('0a') + \
              bytes.fromhex(hex(len(pb_str)).replace('0x','')) + pb_str
            return slspb.parse_pb([pb_str_list, 1])

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            for i in range(0,100):
                datas = [
                    0, # do not compress
                    '%s' % i, # topic
                    '127.0.0.1', # source
                    [("tag1", "mytag1"),("tag2","mytag2")], # tag
                    [int(time.time())], # log time
                    [[("mytest","%s" % i)]] # log key value
                ]
                futures.append(executor.submit(_pb_parse_write, datas))
            success_cnt = 0
            for f in as_completed(futures):
                (logs, loggroup_cnt) = f.result()
                self.assertEqual(1, loggroup_cnt)
                self.assertEqual(1, len(logs))
                self.assertEqual(logs[0]['mytest'],logs[0]['__topic__'])
                self.assertEqual(logs[0]['__source__'],'127.0.0.1')
                success_cnt += 1
            self.assertEqual(success_cnt, 100)

if __name__ == '__main__':
    unittest.main()
