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
        self.client = LogClient(self.endpoint, self.access_key_id, self.access_key_secret)
    
    def tearDown(self):
        pass

    def test_get_logs_v3_response(self):
        body = """
        {
            "meta": 
            {
                "count": 1,
                "progress": "Complete",
                "processedRows": 1,
                "processedBytes": 10,
                "elapsedMillisecond": 1431,
                "hasSQL": false,
                "telementryType": "logging",
                "cpuSec": 0.3,
                "cpuCores": 0.07,
                "mode": 1,
                "scanBytes": 10920,
                "powerSql": false,
                "marker": "test",
                "shard": 3,
                "isAccurate": false,
                "columnTypes": ["int"],
                "whereQuery": "xxx-yyy and LogStore: test-logstore and ErrorCode : RequestTimeExpired | with_pack_meta",
                "aggQuery": "",
                "keys": 
                [
                    "__THREAD__",
                    "Method",
                    "Status",
                    "ClientIP"
                ],
                "terms": [
                    {
                        "term": "xxx-yyy",
                        "key": ""
                    },
                    {
                        "term": "test-logstore",
                        "key": "LogStore"
                    },
                    {
                        "term": "requesttimeexpired",
                        "key": "ErrorCode"
                    }
                ],
                "phraseQueryInfo": {
                    "scanAll": true,
                    "beginOffset": 5,
                    "endOffset": 7,
                    "endTime": 1000
                }
            },
            "data": 
            [
                {
                    "__THREAD__": "10000",
                    "Method": "PostLogStoreLogs",
                    "Status": "400",
                    "ClientIP": "0.0.0.0",
                    "Latency": "96",
                    "NetFlow": "123",
                    "UserId": "-1",
                    "AliUid": "",
                    "Acl": "0",
                    "AccessKeyId": "test-fake-access-key-id",
                    "Owner": "10000000000",
                    "CallerType": "Parent",
                    "ProjectName": "xxx-yyy",
                    "ProjectId": "99999",
                    "UserAgent": "log-c-lite_0.1.0",
                    "APIVersion": "0.6.0",
                    "RequestId": "AAAAAAAAAAAAAAAAAAAA",
                    "Source": "undefined",
                    "OutFlow": "161",
                    "ExOutFlow": "161",
                    "InFlow": "855",
                    "Lines": "1",
                    "LogStore": "test-logstore",
                    "RequestType": "write",
                    "Shard": "0",
                    "Topic": "test-topic",
                    "ErrorCode": "RequestTimeExpired",
                    "ErrorMsg": "request timeFri, 25 Aug 2020 00:00:00 GMT has been expired while server time is Fri, 25 Aug 2023 08:00:00 GMT",
                    "microtime": "1333333329568613",
                    "__pack_meta__": "62|AAAAAAAAAAAAAAAAA==|352|180",
                    "__topic__": "",
                    "__source__": "0.0.0.0",
                    "__tag__:__hostname__": "x.y.z",
                    "__tag__:__path__": "/xxxx.LOG",
                    "__tag__:__pack_id__": "AAAAAAAAAAA-28F",
                    "__time__": "1692000029"
                }
            ]
        }
        """
        self.maxDiff = None
        data = json.loads(body)
        v3_resp = GetLogsV3Response(data, {})
        meta = v3_resp.get_meta()
        logs = v3_resp.get_logs()
        self.assertEqual(1, len(logs))
        self.assertEqual(True, meta.is_completed())
        d = v3_resp.to_dict()
        
        # skip cmp None from dict d and data
        for key in six.iterkeys(d["meta"]):
            if d["meta"][key] is None and key not in data["meta"]:
                data["meta"][key] = None

        for key in six.iterkeys(data["meta"]):
            if data["meta"][key] is None and key not in d["meta"]:
                d["meta"][key] = None

        self.assertDictEqual(d, data)
        
        v2_resp = GetLogsResponse.from_v3_response(v3_resp=v3_resp)
    

    def test_get_logs(self): 
        # send logs
        for i in range(2):
            logitemList = []  # LogItem list
            for j in range(55):
                contents = [('index', str(i * 100 + j))]
                logItem = LogItem()
                logItem.set_time(int(time.time()))
                logItem.set_contents(contents)
                logitemList.append(logItem)
            req2 = PutLogsRequest(self.project, self.logstore, '', None, logitemList)
            self.client.put_logs(req2)

            
        request = GetLogsRequest(self.project, self.logstore,
                                 fromTime=int(time.time()) - 900, 
                                 toTime=int(time.time()),
                                 query=None, line=100, offset=0)   
        time.sleep(3)
        # get logs
        response = self.client.get_logs(request)
        self.assertTrue(response.is_completed())
        self.assertGreaterEqual(response.get_count(), 10)

        # get log
        response = self.client.get_log(self.project, self.logstore, 
                                       int(time.time() - 900),
                                       int(time.time()), 
                                       topic='', query=None, size = 100)
        self.assertTrue(response.is_completed())
        self.assertGreaterEqual(response.get_count(), 10)
        # get log all
        all_logs = self.client.get_log_all(self.project, self.logstore, 
                                       int(time.time() - 900),
                                       int(time.time()))
        count = 0
        for logs in all_logs:
            count += logs.get_count()
            print('get logs: ', count)
            self.assertTrue(logs.is_completed())

        self.assertGreaterEqual(count, 110)    
        # get log all v2
        all_logs = self.client.get_log_all_v2(self.project, self.logstore, 
                                       int(time.time() - 900),
                                       int(time.time()))
        count = 0
        for logs in all_logs:
            count += logs.get_count()
            print('get logs: ', count)
            self.assertTrue(logs.is_completed())

        self.assertGreaterEqual(count, 110) 
        
        # get log v3
        
        all_logs = self.client.get_log_all_v3(self.project, self.logstore, 
                                       int(time.time() - 900),
                                       int(time.time()))
        count = 0
        for logs in all_logs:
            count += logs.get_meta().get_count()
            print('get logs: ', count)
            self.assertTrue(logs.is_completed())
        self.assertGreaterEqual(count, 110) 
        # get log v3
        request.set_line(200)
        response = self.client.get_log_v3(request=request)
        self.assertTrue(response.is_completed())
        self.assertGreater(response.get_meta().get_count(), 100)
        

        

if __name__ == '__main__':
    unittest.main()
