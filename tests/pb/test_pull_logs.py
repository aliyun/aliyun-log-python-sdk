

import unittest
import six

from aliyun.log.logitem import LogItem
from aliyun.log.putlogsrequest import PutLogsRequest
from aliyun.log.util import PbCodec

def _get_binary(key):
    if isinstance(key, six.text_type):
        return key.encode('utf-8')
    return key

class TestPullLogs(unittest.TestCase):
    def test_put_log(self):
        topics = ['a', '', None, 'topic']
        sources = ['', None, 'source', '127.0.0.1']
        item = LogItem(contents=[('key', 'value'),
                                 ('key', 'value inbytes'.encode('utf8')),
                                 ('key', '中文字符'),
                                 ('key', '中文'.encode('utf8'))
                                 ])
        tags = [None, [], [('key', 'value')], ]
        for topic in topics:
            for src in sources:
                for tag in tags:
                    req = PutLogsRequest('test-project', 'logstore', topic, src, logitems=[item] * 100, compress=False, logtags=tag)
                    res = PbCodec.serialize(req, src)
                    from .log_logs_raw_pb2 import LogGroupRaw as LogGroup
                    logGroup = LogGroup()
                    logGroup.ParseFromString(res)
                    self.assertEqual(logGroup.Topic or '', topic or '')
                    self.assertEqual(logGroup.Source or '', src or '')
                    self.assertEqual(len(logGroup.Logs or []), len(req.get_log_items() or []))
                    self.assertEqual(len(logGroup.LogTags or []), len(req.get_log_tags() or []))

                    for i in range(len(logGroup.LogTags or [])):
                        lhs , rhs = logGroup.LogTags[i], req.get_log_tags()[i]
                        self.assertEqual(lhs.Key, rhs[0])
                        self.assertEqual(lhs.Value, rhs[1])
    
                    for i in range(len(logGroup.Logs or [])):
                        lhs , rhs = logGroup.Logs[i], req.get_log_items()[i]
                        self.assertEqual(lhs.Time, rhs.get_time())
                        for j in range(len(lhs.Contents or [])):
                            lhs_contents, rhs_contents = lhs.Contents[j], rhs.get_contents()[j]
                            self.assertEqual(lhs_contents.Key, rhs_contents[0])
                            self.assertEqual(lhs_contents.Value, _get_binary(rhs_contents[1]))
        

if __name__ == '__main__':
    unittest.main()