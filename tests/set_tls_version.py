# -*- coding: utf-8 -*-
import os
import ssl
from aliyun import log
from requests.adapters import HTTPAdapter

access_key_id = os.getenv('LOG_TEST_ACCESS_KEY_ID', '<你的AccessKeyId>')
access_key_secret = os.getenv('LOG_TEST_ACCESS_KEY_SECRET', '<你的AccessKeySecret>')
project_name = os.getenv('LOG_TEST_PROJECT', '<你的Project>')
endpoint = os.getenv('LOG_TEST_ENDPOINT', '<你的访问域名>')

# 确认上面的参数都填写正确了
for param in (access_key_id, access_key_secret, project_name, endpoint):
    assert '<' not in param, '请设置参数：' + param


# 自定义ssl adapter，这里仅以设置ssl_version为例说明
class SSLAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        kwargs["ssl_version"] = ssl.PROTOCOL_TLSv1_2
        return super().init_poolmanager(*args, **kwargs)


# 创建session对象，通过session自定义adapter
session = log.Session(adapter=SSLAdapter())

client = log.LogClient(endpoint, access_key_id, access_key_secret, session=session)
client.create_project(project_name, "hello")
