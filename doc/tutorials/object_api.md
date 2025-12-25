# Object API 使用指南

## 概述

Object API 允许你在 SLS LogStore 中存储和检索任意对象数据。

## API 方法

### put_object

上传一个对象到指定的 LogStore，支持携带自定义 header 元数据。

**方法签名：**

```python
def put_object(self, project_name, logstore_name, object_name, content, headers=None):
    """
    :type project_name: string
    :param project_name: 项目名称

    :type logstore_name: string
    :param logstore_name: LogStore 名称

    :type object_name: string
    :param object_name: 对象名称（只允许 a-z A-Z 0-9 _ -）

    :type content: bytes/string
    :param content: 对象内容（可以为空）

    :type headers: dict
    :param headers: 可选的请求头（所有 headers 都会透传给服务端）
                    - x-log-meta-* 开头的 header 是对象的元数据，会在 get_object 时返回
                    - Content-Type 请求类型
                    - Content-MD5 MD5 值

    :return: PutObjectResponse

    :raise: LogException
    """
```

**示例：**

```python
from aliyun.log import LogClient

# 初始化客户端
client = LogClient(endpoint, access_key_id, access_key)

# 示例 1: 上传简单文本对象
object_name = 'my_object'
content = b'Hello, World!'
response = client.put_object(project, logstore, object_name, content)
print('ETag:', response.get_etag())

# 示例 2: 上传带元数据的对象
headers = {
    'Content-Type': 'text/plain',
    'x-log-meta-author': 'user123',
    'x-log-meta-version': '1.0'
}
response = client.put_object(project, logstore, object_name, content, headers)

# 示例 3: 上传带 Content-MD5 的对象
import hashlib
import base64

md5_hash = hashlib.md5(content).digest()
content_md5 = base64.b64encode(md5_hash).decode('utf-8')

headers = {
    'Content-MD5': content_md5,
    'Content-Type': 'application/octet-stream'
}
response = client.put_object(project, logstore, object_name, content, headers)
```

**HTTP 请求**

```http
PUT /logstores/{logstore}/objects/{key}
Host: {project}.{endpoint}
Content-Type: image/jpg
Content-Length: 344606
x-log-meta-a: a
x-log-meta-b: b

<对象字节流>
```

**HTTP 响应**

```http
HTTP/1.1 200 OK
x-log-request-id: 68FA41E1543C150DB612D36B
Date: Fri, 24 Feb 2012 06:38:30 GMT
Last-Modified: Fri, 24 Feb 2012 06:07:48 GMT
ETag: "5B3C1A2E0563E1B002CC607C*****"
Content-Type: image/jpg
Content-Length: 344606
Server: AliyunSLS
```

### get_object

从指定的 LogStore 获取一个对象。

**方法签名：**

```python
def get_object(self, project_name, logstore_name, object_name):
    """
    :type project_name: string
    :param project_name: 项目名称

    :type logstore_name: string
    :param logstore_name: LogStore 名称

    :type object_name: string
    :param object_name: 对象名称

    :return: GetObjectResponse

    :raise: LogException
    """
```

**示例：**

```python
from aliyun.log import LogClient

# 初始化客户端
client = LogClient(endpoint, access_key_id, access_key)

# 获取对象
object_name = 'my_object'
response = client.get_object(project, logstore, object_name)

# 访问响应数据
print('ETag:', response.get_etag())
print('Last Modified:', response.get_last_modified())
print('Content Type:', response.get_content_type())
print('Content Length:', len(response.get_body()))
print('Content:', response.get_body())

# 获取所有响应头
headers = response.get_headers()
for key, value in headers.items():
    print('{}: {}'.format(key, value))

```

**HTTP 请求**

```http
GET /logstores/{logstore}/objects/{key}
Host: {project}.{endpoint}
```

**HTTP 响应**

```http
HTTP/1.1 200 OK
x-log-request-id: 68FA41E1543C150DB612D36B
Date: Fri, 24 Feb 2012 06:38:30 GMT
Last-Modified: Fri, 24 Feb 2012 06:07:48 GMT
ETag: "5B3C1A2E0563E1B002CC607C*****"
Content-Type: image/jpg
Content-Length: 344606
x-log-meta-a: a
x-log-meta-b: b

[344606 bytes of object data]
```

## 完整示例

参见 [sample_object_api](../../tests/sample_object_api.py) 示例程序。
