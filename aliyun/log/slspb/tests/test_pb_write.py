import os
import unittest

import time
import slspb
import gzip
import pickle
import warnings


class TestWritePB(unittest.TestCase):

    def test_slspb_writepb(self):
        file_dir = os.path.split(os.path.realpath(__file__))[0]

        sample_file = open(file_dir + '/sample_write', 'rb')
        datas = pickle.loads(gzip.decompress(sample_file.read()))
        sample_file.close()

        # print(datas)
        datas[0] = 0 # do not compress
        (pb_str, raw_size, skip_cnt, warnings) = slspb.write_pb(datas)
        self.assertEqual(65741, raw_size)
        self.assertEqual(65741, len(pb_str))

        datas[0] = 1 # do compress
        (pb_str, raw_size, skip_cnt, warnings) = slspb.write_pb(datas)
        self.assertEqual(65741, raw_size)
        self.assertEqual(577, len(pb_str))
    
    def test_slspb_write_time_nano_part(self):
        (pb_str, raw_size, skip_cnt, warnings) = slspb.write_pb([0,None,'128.9.9.1',[("4","5"),("6","7")],[1597317524],[701068179],[[("a","b")]]])
        self.assertEqual(pb_str,b'\n\x13\x08\x94\xc3\xd4\xf9\x05%\x93s\xc9)\x12\x06\n\x01a\x12\x01b\x1a\x00"\t128.9.9.12\x06\n\x014\x12\x0152\x06\n\x016\x12\x017')
        (pb_str, raw_size, skip_cnt, warnings) = slspb.write_pb([0,None,'128.9.9.1',[("4","5"),("6","7")],[1597317524,1701068179],[701068179,597317524],[[("a","b")],[("b","a")]]])
        self.assertEqual(pb_str,b'\n\x13\x08\x94\xc3\xd4\xf9\x05%\x93s\xc9)\x12\x06\n\x01a\x12\x01b\n\x13\x08\x93\xfb\x90\xab\x06%\x94W\x9a#\x12\x06\n\x01b\x12\x01a\x1a\x00"\t128.9.9.12\x06\n\x014\x12\x0152\x06\n\x016\x12\x017')

    def test_slspb_write_rawpb_compare(self):
        (pb_str, raw_size, skip_cnt, warnings) = slspb.write_pb([0,None,'128.9.9.1',[("4","5"),("6","7")],[1597317524],[[("a","b")]]])
        self.assertEqual(pb_str, b'\n\x0e\x08\x94\xc3\xd4\xf9\x05\x12\x06\n\x01a\x12\x01b\x1a\x00"\t128.9.9.12\x06\n\x014\x12\x0152\x06\n\x016\x12\x017')

        (pb_str, raw_size, skip_cnt, warnings) = slspb.write_pb([0,None,'110.30.1.32',[("4","5"),("6","7")],[1597317524],[[("a","b")]]])
        self.assertEqual(pb_str, b'\n\x0e\x08\x94\xc3\xd4\xf9\x05\x12\x06\n\x01a\x12\x01b\x1a\x00"\x0b110.30.1.322\x06\n\x014\x12\x0152\x06\n\x016\x12\x017')

        (pb_str, raw_size, skip_cnt, warnings) = slspb.write_pb([0,"hello",'127.0.0.1',[("4","5"),("6","7")],[1597317524],[[("a","b")]]])
        self.assertEqual(pb_str, b'\n\x0e\x08\x94\xc3\xd4\xf9\x05\x12\x06\n\x01a\x12\x01b\x1a\x05hello"\t127.0.0.12\x06\n\x014\x12\x0152\x06\n\x016\x12\x017')

        (pb_str, raw_size, skip_cnt, warnings) = slspb.write_pb([0,"hello",'127.0.0.1',[("4","5"),("6","7")],[1597317524],[[("a","b"),("22",b"abc")]]])
        self.assertEqual(pb_str, b'\n\x19\x08\x94\xc3\xd4\xf9\x05\x12\x06\n\x01a\x12\x01b\x12\t\n\x0222\x12\x03abc\x1a\x05hello"\t127.0.0.12\x06\n\x014\x12\x0152\x06\n\x016\x12\x017')

        (pb_str, raw_size, skip_cnt, warnings) = slspb.write_pb([0,"hello",'127.0.0.1',[("4","5"),("6","7")],[1597317524],[[("a","b"),("22",b"abc"),("c","d")]]])
        self.assertEqual(pb_str, b'\n!\x08\x94\xc3\xd4\xf9\x05\x12\x06\n\x01a\x12\x01b\x12\t\n\x0222\x12\x03abc\x12\x06\n\x01c\x12\x01d\x1a\x05hello"\t127.0.0.12\x06\n\x014\x12\x0152\x06\n\x016\x12\x017')

        (pb_str, raw_size, skip_cnt, warnings) = slspb.write_pb([0,"hello",'127.0.0.1',[("4","5"),("6","7")],[1597317524],[[("a","b"),("22",b"abc"),("c","d"),("x",b'abc\xa0abfa')]]])
        self.assertEqual(pb_str, b'\n0\x08\x94\xc3\xd4\xf9\x05\x12\x06\n\x01a\x12\x01b\x12\t\n\x0222\x12\x03abc\x12\x06\n\x01c\x12\x01d\x12\r\n\x01x\x12\x08abc\xa0abfa\x1a\x05hello"\t127.0.0.12\x06\n\x014\x12\x0152\x06\n\x016\x12\x017')

        (pb_str, raw_size, skip_cnt, warnings) = slspb.write_pb([0,"hello",'127.0.0.1',[],[1597317524],[[("a","b"),("22",b"abc"),("c","d"),("x",b'abc\xa0abfa')]]])
        self.assertEqual(pb_str, b'\n0\x08\x94\xc3\xd4\xf9\x05\x12\x06\n\x01a\x12\x01b\x12\t\n\x0222\x12\x03abc\x12\x06\n\x01c\x12\x01d\x12\r\n\x01x\x12\x08abc\xa0abfa\x1a\x05hello"\t127.0.0.1')

        (pb_str, raw_size, skip_cnt, warnings) = slspb.write_pb([0,"",'127.0.0.1',[("tag_k",'txx')],[1597317524],[[("a","b1")]]])
        self.assertEqual(pb_str, b'\n\x0f\x08\x94\xc3\xd4\xf9\x05\x12\x07\n\x01a\x12\x02b1\x1a\x00"\t127.0.0.12\x0c\n\x05tag_k\x12\x03txx')


    def test_bad_unicode_skip(self):
        #
        datas = [
            0, # do not compress
            '', # topic
            '127.0.0.1', # source
            [("tag1", "mytag1")], # tag
            [int(time.time())], # log time
            [[("mytest",u'Hello World! \udd47')]] # log key value
        ]

        (pb_str, raw_size, skip_cnt, warnings) = slspb.write_pb(datas)

        self.assertEqual(len(warnings), 1)
        self.assertEqual(skip_cnt,1)

        datas = [
            0, # do not compress
            '', # topic
            '127.0.0.1', # source
            [("tag1", "mytag1")], # tag
            [int(time.time())], # log time
            [[("mytest",u'Hello World! \udd47'),("mytest",u'Hello World! \udd48')]] # log key value
        ]

        (pb_str, raw_size, skip_cnt, warnings) = slspb.write_pb(datas)
        self.assertEqual(len(warnings), 2)
        self.assertEqual(skip_cnt,1)

        datas = [
            0, # do not compress
            '', # topic
            '127.0.0.1', # source
            [("tag1", "mytag1")], # tag
            [int(time.time())], # log time
            [[("mytest",u'Hello World! \udd47'),("mytest",u'Hello World!')]] # log key value
        ]

        (pb_str, raw_size, skip_cnt, warnings) = slspb.write_pb(datas)

        self.assertEqual(len(warnings), 1)
        self.assertEqual(skip_cnt,0)

        ts = int(time.time())
        datas = [
            0, # do not compress
            '', # topic
            '127.0.0.1', # source
            [("tag1", "mytag1")], # tag
            [ts], # log time
            [[("mytest",u'Hello World! \udd47'),("mytest",u'Hello World! \udd48'),
              ("mytest",u'Hello World!')]] # log key value
        ]

        datas2 = [
            0, # do not compress
            '', # topic
            '127.0.0.1', # source
            [("tag1", "mytag1")], # tag
            [ts], # log time
            [[("mytest",u'Hello World!')]] # log key value
        ]

        (pb_str1, raw_size1, skip_cnt1, warnings1) = slspb.write_pb(datas)
        (pb_str2, raw_size2, skip_cnt2, warnings2) = slspb.write_pb(datas2)

        self.assertEqual(len(warnings1),2)
        self.assertEqual(skip_cnt1,0)

        self.assertEqual(skip_cnt2,0)
        self.assertEqual(pb_str1,pb_str2)

    def test_slspb_ts_value_invalid(self):
        bad_tss = [-2147483649,0,-1,1,4813906245,268435456]
        for ts in bad_tss:
            datas = [
                0, # do not compress
                '', # topic
                '127.0.0.1', # source
                [("tag1", "mytag1")], # tag
                [ts], # log time
                [[("mytest",u'Hello World!')]] # log key value
            ]

            (pb_str, raw_size, skip_cnt, warnings) = slspb.write_pb(datas)
            self.assertEqual(len(warnings),1)
            self.assertTrue("ts is value invalid, will be reset to system time" in warnings[0])

    def test_slspb_writepb_special_arg(self):
        # this is valid
        (pb_str, raw_size, skip_cnt, warnings) = slspb.write_pb([0,None,None,[("4","5"),("6","7")],[1597317524],[[("a","b")]]])
        self.assertEqual(len(pb_str), raw_size)

        (pb_str, raw_size, skip_cnt, warnings) = slspb.write_pb([0,None,None,[("4",b"abc"),("6","7")],[1597317524],[[("a","b")]]])
        self.assertEqual(len(pb_str), raw_size)

    def test_slspb_writepb_speical_len(self):
        large_str = ''.join(['a' for i in range(0,300)])
        (pb_str, raw_size, skip_cnt, warnings) = slspb.write_pb([0,large_str,None,[("4","5"),("6","7")],[1597317524],[[("a","b")]]])
        self.assertEqual(['topic is too long, limit to 255'], warnings)
        self.assertEqual(301, len(pb_str))

        (pb_str, raw_size, skip_cnt, warnings) = slspb.write_pb([0,None,large_str,[("4","5"),("6","7")],[1597317524],[[("a","b")]]])
        self.assertEqual(['source is too long, limit to 128'], warnings)
        self.assertEqual(165, len(pb_str))

        (pb_str, raw_size, skip_cnt, warnings) = slspb.write_pb([0,None,None,[("4",large_str),("6","7")],[1597317524],[[("a","b")]]])
        self.assertEqual(['tag value is too long, limit to 255'], warnings)
        self.assertEqual(301, len(pb_str))

    def test_slspb_writepb_arg_check(self):
        with self.assertRaises(Exception) as context:
            slspb.write_pb(1)
        self.assertTrue('write_pb except a list' in str(context.exception))

        with self.assertRaises(Exception) as context:
            slspb.write_pb([])
        self.assertTrue('write_pb except a list' in str(context.exception))

        with self.assertRaises(Exception) as context:
            slspb.write_pb(["",2,3,4,5,6])
        self.assertTrue('except int at fisrt element in list' in str(context.exception))

        with self.assertRaises(Exception) as context:
            slspb.write_pb([None,2,3,4,5,6])
        self.assertTrue('except int at fisrt element in list' in str(context.exception))

        with self.assertRaises(Exception) as context:
            slspb.write_pb([30,2,3,4,5,6])
        self.assertTrue('except 0 or 1 at fisrt element in list' in str(context.exception))

        with self.assertRaises(Exception) as context:
            slspb.write_pb([1,2,3,4,5,6])
        # print(str(context.exception))
        self.assertTrue('except string at second element(topic) in list' in str(context.exception))

        # with self.assertRaises(Exception) as context:
        #     slspb.write_pb([1,None,3,4,5,6])
        # self.assertTrue('except string at second element(topic) in list' in str(context.exception))

        with self.assertRaises(Exception) as context:
            slspb.write_pb([1,"topic",3,4,5,6])
        self.assertTrue('except string at third element(source) in list' in str(context.exception))

        # with self.assertRaises(Exception) as context:
        #     slspb.write_pb([1,"topic",None,4,5,6])
        # self.assertTrue('except string at third element(source) in list' in str(context.exception))

        with self.assertRaises(Exception) as context:
            slspb.write_pb([1,"topic","souce",4,5,6])
        self.assertTrue('except tag list at fourth element in list' in str(context.exception))

        with self.assertRaises(Exception) as context:
            slspb.write_pb([1,"topic","souce",[4],5,6])
        self.assertTrue('except tag list at fourth element in list' in str(context.exception))

        with self.assertRaises(Exception) as context:
            slspb.write_pb([1,"topic","souce",[(4,5)],5,6])
        self.assertTrue('except tag list at fourth element in list' in str(context.exception))

        with self.assertRaises(Exception) as context:
            slspb.write_pb([1,"topic","souce",[("4",5)],5,6])
        self.assertTrue('except tag list at fourth element in list' in str(context.exception))

        with self.assertRaises(Exception) as context:
            slspb.write_pb([1,"topic","souce",[("4","5"),(6,"7")],5,6])
        self.assertTrue('except tag list at fourth element in list' in str(context.exception))

        with self.assertRaises(Exception) as context:
            slspb.write_pb([1,"topic","souce",[("4","5"),(6,"7")],5,0,6])
        self.assertTrue('except tag list at fourth element in list' in str(context.exception))

        with self.assertRaises(Exception) as context:
            slspb.write_pb([1,"topic","souce",[("4","5"),("6","7")],5,6])
        self.assertTrue('except time list at fifth element in list' in str(context.exception))

        with self.assertRaises(Exception) as context:
            slspb.write_pb([1,"topic","souce",[("4","5"),("6","7")],5,0,6])
        self.assertTrue('except time list at fifth element in list' in str(context.exception))

        with self.assertRaises(Exception) as context:
            slspb.write_pb([1,"topic","souce",[("4","5"),("6","7")],[int(time.time())],6])
        self.assertTrue('except content list at sixth element in list' in str(context.exception))

        with self.assertRaises(Exception) as context:
            slspb.write_pb([1,"topic","souce",[("4","5"),("6","7")],[int(time.time())],[0],6])
        self.assertTrue('except content list at seventh element in list' in str(context.exception))

        with self.assertRaises(Exception) as context:
            slspb.write_pb([1,"topic","souce",[("4","5"),("6","7")],[int(time.time())],6,[[("a","b")]]])
        self.assertTrue('except time_ns_part list at sixth element in list' in str(context.exception))
        
        with self.assertRaises(Exception) as context:
            slspb.write_pb([1,"topic","souce",[("4","5"),("6","7")],[int(time.time())],{"a":"b"},[[("a","b")]]])
        self.assertTrue('except time_ns_part list at sixth element in list' in str(context.exception))

        with self.assertRaises(Exception) as context:
            slspb.write_pb([1,"topic","souce",[("4","5"),("6","7")],[int(time.time())],[int(time.time())],7])
        self.assertTrue('except content list at seventh element in list' in str(context.exception))

        with self.assertRaises(Exception) as context:
            slspb.write_pb([1,"topic","souce",[("4","5"),("6","7")],[int(time.time())],[6,7]])
        self.assertTrue('the count of ts and logs mismath' in str(context.exception))

        with self.assertRaises(Exception) as context:
            slspb.write_pb([1,"topic","souce",[("4","5"),("6","7")],["1","2"],[6,7]])
        self.assertTrue('except time list at fifth element in list' in str(context.exception))

        with self.assertRaises(Exception) as context:
            slspb.write_pb([1,"topic","souce",[("4","5"),("6","7")],[int(time.time())],[6]])
        self.assertTrue('except content list at sixth element in list' in str(context.exception))

        with self.assertRaises(Exception) as context:
            slspb.write_pb([1,"topic","souce",[("4","5"),("6","7")],[int(time.time())],[int(time.time())],[6]])
        self.assertTrue('except content list at seventh element in list' in str(context.exception))

        with self.assertRaises(Exception) as context:
            slspb.write_pb([1,"topic","souce",[("4","5"),("6","7")],[int(time.time())],[[1]]])
        self.assertTrue('except content list at sixth element in list' in str(context.exception))

        with self.assertRaises(Exception) as context:
            slspb.write_pb([1,"topic","souce",[("4","5"),("6","7")],[int(time.time())],[int(time.time())],[[1]]])
        self.assertTrue('except content list at seventh element in list' in str(context.exception))

        with self.assertRaises(Exception) as context:
            slspb.write_pb([1,"topic","souce",[("4","5"),("6","7")],[int(time.time())],[[(1,2,3)]]])
        self.assertTrue('except content list at sixth element in list' in str(context.exception))

        with self.assertRaises(Exception) as context:
            slspb.write_pb([1,"topic","souce",[("4","5"),("6","7")],[int(time.time())],[int(time.time())],[[(1,2,3)]]])
        self.assertTrue('except content list at seventh element in list' in str(context.exception))

        (pb_str, raw_size, skip_cnt, warnings) = slspb.write_pb([0,"topic","souce",[("4","5"),("6","7")],[int(time.time())],[[(1,3)]]])
        self.assertTrue(skip_cnt == 1)
        self.assertTrue(len(warnings) == 1)
        self.assertTrue("slspb skip key warning: except content key be string" in warnings[0])

        (pb_str, raw_size, skip_cnt, warnings) = slspb.write_pb([0,"topic","souce",[("4","5"),("6","7")],[int(time.time())],[0],[[(1,3)]]])
        self.assertTrue(skip_cnt == 1)
        self.assertTrue(len(warnings) == 1)
        self.assertTrue("slspb skip key warning: except content key be string" in warnings[0])

        with self.assertRaises(Exception) as context:
            slspb.write_pb([1,"topic","souce",[("4","5"),("6","7")],[int(time.time())],[[("1",3)]]])
        self.assertTrue('except content list at sixth element in list' in str(context.exception))

        with self.assertRaises(Exception) as context:
            slspb.write_pb([1,"topic","souce",[("4","5"),("6","7")],[int(time.time())],[0],[[("1",3)]]])
        self.assertTrue('except content list at seventh element in list' in str(context.exception))

        (pb_str, raw_size, skip_cnt, warnings) = slspb.write_pb([1,"topic","souce",[("4","5"),("6","7")],[int(time.time())],[[("1","3"),(5,6)]]])
        self.assertTrue(skip_cnt == 0)
        self.assertTrue(len(warnings) == 1)
        self.assertTrue("slspb skip key warning: except content key be string" in warnings[0])

        (pb_str, raw_size, skip_cnt, warnings) = slspb.write_pb([1,"topic","souce",[("4","5"),("6","7")],[int(time.time())],[0],[[("1","3"),(5,6)]]])
        self.assertTrue(skip_cnt == 0)
        self.assertTrue(len(warnings) == 1)
        self.assertTrue("slspb skip key warning: except content key be string" in warnings[0])

        with self.assertRaises(Exception) as context:
            slspb.write_pb([1,"topic","souce",[("4","5"),("6","7")],[int(time.time())],[[("1","3"),("5",6)]]])
        self.assertTrue('except content list at sixth element in list' in str(context.exception))

        with self.assertRaises(Exception) as context:
            slspb.write_pb([1,"topic","souce",[("4","5"),("6","7")],[int(time.time())],[0],[[("1","3"),("5",6)]]])
        self.assertTrue('except content list at seventh element in list' in str(context.exception))

        with self.assertRaises(Exception) as context:
            slspb.write_pb([1,"topic","souce",[("4","5"),("6","7")],[int(time.time())],[[("1","3"),("5",[])]]])
        self.assertTrue('except content list at sixth element in list' in str(context.exception))

        with self.assertRaises(Exception) as context:
            slspb.write_pb([1,"topic","souce",[("4","5"),("6","7")],[int(time.time())],[0],[[("1","3"),("5",[])]]])
        self.assertTrue('except content list at seventh element in list' in str(context.exception))

        with self.assertRaises(Exception) as context:
            slspb.write_pb([1,"topic","souce",[("4","5"),("6","7")],[],[]])
        self.assertTrue('the count of ts and logs should large than 1' in str(context.exception))

        with self.assertRaises(Exception) as context:
            slspb.write_pb([0,None,'128.9.9.1',[("4","5"),("6","7")],[1597317524],[[("a",1)]]])
        self.assertTrue('content value should be string or bytes' in str(context.exception))

        with self.assertRaises(Exception) as context:
            slspb.write_pb([0,None,'128.9.9.1',[("4","5"),("6","7")],[1597317524],[0],[[("a",1)]]])
        self.assertTrue('content value should be string or bytes' in str(context.exception))

        with self.assertRaises(Exception) as context:
            slspb.write_pb([0,None,'128.9.9.1',[("4","5"),("6","7")],[1597317524],[[("a",1),("xx","ww")]]])
        self.assertTrue('content value should be string or bytes' in str(context.exception))

        with self.assertRaises(Exception) as context:
            slspb.write_pb([0,None,'128.9.9.1',[("4","5"),("6","7")],[1597317524],[0],[[("a",1),("xx","ww")]]])
        self.assertTrue('content value should be string or bytes' in str(context.exception))

        with self.assertRaises(Exception) as context:
            slspb.write_pb([0,None,'128.9.9.1',[("4","5"),("6","7")],[1597317524],[[("a",1.1)]]])
        self.assertTrue('content value should be string or bytes' in str(context.exception))


        with self.assertRaises(Exception) as context:
            slspb.write_pb([0,None,'128.9.9.1',[("4","5"),("6","7")],[1597317524],[0],[[("a",1.1)]]])
        self.assertTrue('content value should be string or bytes' in str(context.exception))

if __name__ == '__main__':
    unittest.main()
