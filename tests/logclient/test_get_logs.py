import unittest

import os
import json
from aliyun.log import LogClient, GetLogsV3Response, GetLogsResponse, GetLogsRequest, PutLogsRequest, LogItem
import six
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

    def tearDown(self):
        pass

    def test_get_log(self):
        print('test_get_log')
        resp = self.client.get_log(self.project, self.logstore,
                            self.from_time, self.to_time,
                            query="*",
                            reverse=True,
                            offset=0,
                            size=100,
                            power_sql=True,
                            scan=True,
                            accurate_query=True,
                            )
        resp.log_print()
        print('second query')
        resp = self.client.get_log(self.project, self.logstore,
                            self.from_time, self.to_time,
                            query="*",
                            reverse=False,
                            offset=50,
                            size=100,
                            power_sql=True,
                            scan=False,
                            accurate_query=True,
                            )
        resp.log_print()
                
    def test_get_logs(self):
        print('test_get_logs')
        request=GetLogsRequest(project=self.project,
                               logstore=self.logstore,
                               fromTime=self.from_time,
                               toTime=self.to_time,
                               query="*",
                               reverse=True,
                               offset=0,
                               line=100,
                               power_sql=True,
                               scan=True,
                               accurate_query=True,
                               )
        resp = self.client.get_logs(request)
        resp.log_print()
    
    def test_get_log_all_v2(self):
        print('test_get_log_all_v2')
        iter = self.client.get_log_all_v2(self.project, self.logstore,
                                          self.to_time - 30, self.to_time,
                                          query="*",
                                          reverse=True,
                                          offset=0,
                                          power_sql=True,
                                          scan=True)
        for logs in iter:
            logs.log_print()

    def test_get_log_all_v3(self):
        print('test_get_log_all_v3')
        iter = self.client.get_log_all_v3(self.project, self.logstore,
                                          self.to_time - 30, self.to_time,
                                          query="*",
                                          reverse=True,
                                          offset=0,
                                          power_sql=True,
                                          scan=True)
        logs1 = [*iter]
        iter = self.client.get_log_all_v2(self.project, self.logstore,
                                          self.to_time - 30, self.to_time,
                                          query="*",
                                          reverse=True,
                                          offset=0,
                                          power_sql=True,
                                          scan=True)
        logs2 = [*iter]
        self.assertEquals(len(logs1), len(logs2))
        for resp1, resp2 in zip(logs1, logs2):
            for log1, log2 in zip(resp1.get_logs(), resp2.get_logs()):
                self.assertLogEquals(log1, log2)
        
    def assertLogEquals(self, log1, log2):
        self.assertEquals(log1.timestamp, log2.timestamp)
        self.assertEquals(log1.source, log2.source)
        contents1 = log1.contents
        contents2 = log2.contents
        for content1, content2 in zip(contents1, contents2):
            self.assertEquals(content1[0], content2[0])
            self.assertEquals(content1[1], content2[1])

    def test_get_log_v3(self):
        print('test_get_log_v3')
        resp1 = self.client.get_log(self.project, self.logstore,
                            self.from_time, self.to_time,
                            query="*",
                            reverse=True,
                            offset=50,
                            size=100,
                            power_sql=True,
                            scan=True,
                            accurate_query=True,
                            )
        resp2 = self.client.get_log_v3(self.project, self.logstore,
                            self.from_time, self.to_time,
                            query="*",
                            reverse=True,
                            offset=50,
                            size=100,
                            power_sql=True,
                            scan=True,
                            accurate_query=True,
                            )
        self.assertEquals(resp1.get_count(), resp2.get_count())
        logs1 = resp1.get_logs()
        logs2 = resp2.get_logs()
        for log1, log2 in zip(logs1, logs2):
            self.assertLogEquals(log1, log2)
if __name__ == '__main__':
    unittest.main()
