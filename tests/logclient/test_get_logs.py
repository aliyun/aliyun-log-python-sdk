import unittest

import os
from aliyun.log import LogClient
from aliyun.log.util import Util
import time


class TestDict(unittest.TestCase):

    def setUp(self):
        self.logstore = os.environ.get('TEST_LOGSTORE')
        self.project = os.environ.get('TEST_PROJECT')
        self.access_key_id = os.environ.get('TEST_ACCESS_KEY_ID')
        self.access_key_secret = os.environ.get('TEST_ACCESS_KEY_SECRET')
        self.endpoint = os.environ.get('TEST_ENDPOINT')
        self.client = LogClient(
            self.endpoint, self.access_key_id, self.access_key_secret)
        self.to_time = int(time.time())
        self.from_time = self.to_time - 3600
        print('lz4 is available:', Util.is_lz4_available())

    def tearDown(self):
        pass

    def compare(self, fn, *args, **kwargs):
        self.client._get_log_use_post = False
        respOld = fn(*args, **kwargs)
        self.client._get_log_use_post = True
        respNew = fn(*args, **kwargs)
        self.assertCompatibility(respNew, respOld)
        respNew.log_print()
    
    def compare_generator(self, fn, *args, **kwargs):
        self.client._get_log_use_post = False
        respOld = [*fn(*args, **kwargs)]
        self.client._get_log_use_post = True
        respNew = [*fn(*args, **kwargs)]
        self.assertTrue(len(respNew) - len(respOld) <= 1 and len(respNew) - len(respOld) >= -1)
        for resp1, resp2 in zip(respOld, respNew):
            self.assertCompatibility(resp1, resp2)
            resp1.log_print()
        
    def test_get_log1(self):
        print('test_get_log1')
        self.compare(self.client.get_log, self.project, self.logstore,
                     self.from_time, self.to_time,
                     query="*",
                     reverse=True,
                     offset=0,
                     size=100,
                     power_sql=True,
                     scan=False,
                     accurate_query=True)

    def test_get_log2(self):
        print('test_get_log2')
        self.compare(self.client.get_log, self.project, self.logstore,
                     self.from_time, self.to_time,
                     query="* | select * limit 10,10",
                     reverse=False,
                     power_sql=True,
                     scan=False,
                     accurate_query=True)

    def test_get_log3(self):
        print('test_get_log3')
        self.compare(self.client.get_log, self.project, self.logstore,
                     self.from_time, self.to_time,
                     query="* | select method limit 10",
                     reverse=True,
                     power_sql=True,
                     scan=True,
                     accurate_query=True)
    
    def test_get_log4(self):
        print('test_get_log4')
        self.compare(self.client.get_log, self.project, self.logstore,
                     self.from_time, self.to_time,
                     query="*",
                     offset=0,
                     size=10,
                     reverse=True,
                     power_sql=True,
                     scan=True,
                     accurate_query=True)

    def test_get_log5(self):
        print('test_get_log5')
        self.compare_generator(self.client.get_log_all_v2, self.project, self.logstore,
                     self.from_time, self.to_time,
                     query="*",
                     offset=0,
                     reverse=False,
                     power_sql=False,
                     scan=False,
                     forward=False)

    def test_get_log6(self):
        print('test_get_log6')
        self.compare_generator(self.client.get_log_all_v2, self.project, self.logstore,
                     self.from_time, self.to_time,
                     query="* | select method",
                     offset=0,
                     reverse=True,
                     power_sql=True,
                     scan=True,
                     forward=False)
        
    def assertCompatibility(self, resp1, resp2):
        methods = ['is_completed', 'get_count', 'get_processed_rows',
                   'get_has_sql', 'get_scan_all',
                   'get_query_mode',
                   'get_begin_offset', 'get_end_offset',]
        for method in methods:
            msg = "method: %s \n resp1Meta: %s \n resp1Header: %s \n resp2Meta: %s \n resp2Header: %s" % (method, 
                resp1.get_meta()._to_dict(), resp1.get_all_headers(),
                resp2.get_meta()._to_dict(), resp2.get_all_headers())
            self.assertEqual(getattr(resp1, method)(), getattr(resp2, method)(), msg)
        self.assertEqual(resp1.get_where_query().strip(), resp2.get_where_query().strip(), 'get_where_query')
        self.assertEqual(resp1.get_agg_query().strip(), resp2.get_agg_query().strip(), 'get_agg_query')
        
        self.assertLogEquals(resp1, resp2)
    
    def assertLogEquals(self, resp1, resp2):
        msg = '%s or %s' % (resp1.get_request_id(), resp2.get_request_id())
        for log1, log2 in zip(resp1.get_logs(), resp2.get_logs()):
            self.assertEqual(log1.timestamp, log2.timestamp, msg)
            self.assertEqual(log1.source, log2.source, msg)
            contents1 = log1.contents
            contents2 = log2.contents
            for content1, content2 in zip(contents1, contents2):
                self.assertEqual(content1[0], content2[0], msg)
                self.assertEqual(content1[1], content2[1], msg)

if __name__ == '__main__':
    unittest.main()
