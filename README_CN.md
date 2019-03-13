# User Guide (中文)

[![Documentation Status](https://readthedocs.org/projects/aliyun-log-python-sdk/badge/?version=latest)](http://aliyun-log-python-sdk.readthedocs.io/?badge=latest)
[![Pypi Version](https://badge.fury.io/py/aliyun-log-python-sdk.svg)](https://badge.fury.io/py/aliyun-log-python-sdk)
[![Travis CI](https://travis-ci.org/aliyun/aliyun-log-python-sdk.svg?branch=master)](https://travis-ci.org/aliyun/aliyun-log-python-sdk)
[![Development Status](https://img.shields.io/pypi/status/aliyun-log-python-sdk.svg)](https://pypi.python.org/pypi/aliyun-log-python-sdk/)
[![Python version](https://img.shields.io/pypi/pyversions/aliyun-log-python-sdk.svg)](https://pypi.python.org/pypi/aliyun-log-python-sdk/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/aliyun/aliyun-log-python-sdk/blob/master/LICENSE)

[README in English](https://github.com/aliyun/aliyun-log-python-sdk/blob/master/README.md)


## 基本介绍

这是Log Service SDK for Python的开源版本。Log Service SDK for Python是阿里云日志服务
（Log Service）API的Python编程接口，提供了对于Log Service Rest API所有接口的封装
和支持，帮助Python开发人员更快编程使用阿里云Log Service服务。

参考文档: [http://aliyun-log-python-sdk.readthedocs.io/](http://aliyun-log-python-sdk.readthedocs.io/)

不想写代码? 可以试一下[命令行控制台](http://aliyun-log-cli.readthedocs.io/en/latest/), 其覆盖了这个SDK的几乎所有功能.

### 具体功能

1. 封装所有Rest API (管理, 数据操作, 消费组等)
2. 消费组高阶类支持
3. Python日志模块的Handler
4. 高阶操作支持: 自动分页, 自动未完成重试, 复制Project/Logstore配置, 调整更大的Shard读写数, 查看资源使用等.
5. Elasticsearch 数据迁移
6. 数据ETL功能: 按照shard/时间高速跨logstore复制数据, 根据灵活的配置规则, 对数据进行批量或持续可并发的数据ETL转换.

### 支持Python版本

1. Python 2.6
2. Python 2.7
3. Python 3.3
4. Python 3.4
5. Python 3.5
6. Python 3.6
7. Python 3.7
8. Pypy2
9. Pypy3


### 支持API版本

1. Log Service API 0.6

### 版本历史

[版本历史](https://github.com/aliyun/aliyun-log-python-sdk/releases)

## 安装
```shell
pip install -U aliyun-log-python-sdk
```

如果提示`time-out`之类的错误，表示网络不通，建议可以加上国内清华的索引试一试：

```shell
pip install -U aliyun-log-python-sdk -i https://pypi.tuna.tsinghua.edu.cn/simple
```

如果存在安装Regex失败的错误, 可以参考使用`yun`/`apt-get`或者手动安装一下python-devel
https://rpmfind.net/linux/rpm2html/search.php?query=python-devel


## 代码示例
- [代码示例](https://github.com/aliyun/aliyun-log-python-sdk/tree/master/tests)

## 完整API参考
- [API参考](http://aliyun-log-python-sdk.readthedocs.io/api.html)


## 配置SDK
参考[SDK配置](https://help.aliyun.com/document_detail/29064.html?spm=5176.doc29068.2.8.SWJhYZ)
获得访问秘钥的ID和Key以及访问入口Endpoint, 构建一个LogClient的客户端.

```python
from aliyun.log import LogClient

# “华东 1 (杭州)” Region 的日志服务入口。
endpoint = 'cn-hangzhou.log.aliyuncs.com'
# 用户访问秘钥对中的 AccessKeyId。
accessKeyId = 'ABCDEFGHIJKLJMN'
# 用户访问秘钥对中的 AccessKeySecret。
accessKey = 'OPQRSTUVWXYZ'

client = LogClient(endpoint, accessKeyId, accessKey)
# 使用client的方法来操作日志服务
```

### Https连接
如果要使用https连接, 在配置`endpoint`时, 传入`https://`前缀. 例如:
```python
endpoint = 'https://cn-hangzhou.log.aliyuncs.com'

```

`*.aliyuncs.com`的证书是GlobalSign的, 默认大部分机器已经按照并信任, 如果您的机器没有默认信任, 可以下载证书并安装信任, 可以参考[这个文档](https://success.outsystems.com/Support/Enterprise_Customers/Installation/Install_a_trusted_root_CA__or_self-signed_certificate)来操作.



## 数据采集配置
### 管理日志项目

- 获取列表

  列出本region下面的所有可见项目:

```python
res = client.list_project()
res.log_print()
```

  **注意：** 默认获取100个项目，通过传入参数`offset`和`size`来获取更多

- 获取信息

  获取单个项目的较为详细的信息.

```python
res = client.get_project('project1')
res.log_print()
```

- 创建

```python
res = client.create_project("project1", "a simple project")
res.log_print()
```


- 删除

```python
res = client.delete_project("project1")
res.log_print()
```

  **注意：** 只能删除空的项目.

- 复制

  复制一个项目的所有日志库和相应的配置(包括机器组合索引等), 要求目标项目不存在.

```python
res = client.copy_project("project1", "project2")
res.log_print()
```


### 管理日志库(logstore)

日志库属于某一个项目, 所有的操作都需要传入项目名称.

- 获取列表

  获取一个项目下的所有日志库：

```python
from aliyun.log import ListLogstoresRequest
request = ListLogstoresRequest('project1')
res = client.list_logstores(request)
res.log_print()
```

- 创建

  创建一个日志库：

```python
res = client.create_logstore('project1', 'logstore1', ttl=30, shard_count=3)
res.log_print()
```

  **注意：** 参数`ttl`和`shard_count`表示日志存储日期和分区数量.


- 获取信息

  获取单个日志库较为详细的信息.

```python
res = client.get_logstore('project1', 'logstore1')
res.log_print()
```

- 删除

  通过`delete_logstore`删除日志库

- 更新

  通过`update_logstore`删除日志库

### 管理日志库分区(shard)
分区属于某一个日志库, 所有的操作都需要传入项目名称和日志库名称.

- 获取列表

  通过`list_shards`获取列表

- 分裂

  通过`split_shard`分裂分区

- 合并

  通过`merge_shard`合并分区

### 管理日志库Logtail配置
Logtail的配置拥有独立的名字, 但其与日志库(logstore)一般是一一对应的关系.

- 获取列表

	列出本项目下所有Logtail的配置名单:

```python
res = client.list_logtail_config('project1')
res.log_print()
```

  **注意：** 默认获取100个配置项，通过传入参数`offset`和`size`来获取更多

  **输出：**

```json
{"count": 2, "configs": ["config_name1", "config_name2"], "total": 2}
```

- 创建

创建一个Logtail配置, 并关联到日志库上:

```python
from aliyun.log import LogtailConfigGenerator as helper
import json
config_detail_json = """{
  "configName": "json_1",
  "inputDetail": {
    "logType": "json_log",
    "filePattern": "test.log",
    "logPath": "/json_1"
  },
  "inputType": "file",
  "outputDetail": {
    "logstoreName": "logstore3"
  }
}"""
request = helper.generate_config(json.loads(config_detail))
res = client.create_logtail_config('project1', request)
res.log_print()
```

  **注意：**
  - 创建的配置的名字`configName`和关联的日志库名字`logstoreName`都是放在传入的`request`中.
  - 不同类型的Logtail配置参数不一样，可以参考[这篇文章](http://aliyun-log-cli.readthedocs.io/en/latest/tutorials/tutorial_create_logtail_config.html)了解更多业务逻辑。
  - 更多的JSON样例，请参考[这里](https://github.com/aliyun/aliyun-log-cli/tree/master/tests/logtail)。
  - 创建的Logtail的配置还没有应用到任何一个机器组, 需要调用后面的API`apply_config_to_machine_group`来进行配置.


- 获取信息

  获取Logtail配置的具体信息:

```python
res = client.get_logtail_config('project1', 'config1')
res.log_print()
```

- 修改

  通过`update_logtail_config`来修改Logtail配置.

- 删除

  通过`delete_logtail_config`来删除Logtail配置.

### 管理机器组
机器组(MachineGroup)主要是用于应用Logtail配置的. 其与Logtail配置的关系是多对多的关系. 一个Logtail配置可以应用到多个机器组上, 放置一个机器组也可以应用多个Logtail配置.

- 获取列表

  列出本项目下所有机器组的名单:

```python
res = client.list_machine_group('project1')
res.log_print()
```

  **注意：** 默认获取100个机器组，通过传入参数`offset`和`size`来获取更多

  **输出：**

```json
{"count": 2, "machinegroups": ["group_name1", "group_name2"], "total": 2}
```

- 创建

  创建一个机器组:

```python
from aliyun.log import MachineGroupDetail
config_detail_json = {
    "group_name": "group_name1",
    "machine_list": [
      "machine1",
      "machine2"
    ],
    "machine_type": "userdefined",
    "group_type": "Armory",
    "group_attribute": {
      "externalName": "ex name",
      "groupTopic": "topic x"
    }
  }

request = MachineGroupDetail()
request.from_json(config_detail_json)
res = client.create_machine_group('project1', request)
res.log_print()
```

  **注意：**
  - 创建的机器组的名字`group_name`是放在传入的`request`中.
  - 创建的机器组还没有应用到任何一个Logtail配置, 需要调用后面的API`apply_config_to_machine_group`来进行配置.


- 获取信息

  获取机器组的具体信息:

```python
res = client.get_machine_group('project1', 'group1')
res.log_print()
```

- 修改

  通过`update_logtail_config`来修改Logtail配置.

- 删除

  通过`delete_logtail_config`来删除Logtail配置.

### 关联Logtail配置到机器组
机器组与Logtail配置的关系是多对多的关系. 一个Logtail配置可以应用到多个机器组上, 反之一个机器组也可以应用多个Logtail配置.

- 应用Logtail配置到特定机器组

```python
res = client.apply_config_to_machine_group('project1', 'config1', 'group1')
res.log_print()
```

- 去除机器组的Logtail配置

```python
res = client.remove_config_to_machine_group('project1', 'config1', 'group1')
res.log_print()
```

- 获取Logtail配置应用到的机器组名单

```python
res = client.get_config_applied_machine_groups('project1', 'config1')
res.log_print()
```

  **输出：**

```json
{"count": 2, "machinegroups": ["group1", "group2"]}
```

- 获取机器组应用的Logtail配置名单

```python
res = client.get_machine_group_applied_configs('project1', 'group1')
res.log_print()
```

  **输出：**

```json
{"count": 2, "configs": ["config1", "config2"]}
```

### 日志库索引管理
只有配置了索引的日志库才能使用SQL查询日志.

- 创建

给一个日志库创建索引

```python
from aliyun.log import IndexConfig
request_json = {
     "keys": {
       "f1": {
         "caseSensitive": False,
         "token": [
           ",", " ", "\"", "\"", ";", "=",  "(", ")", "[", "]",
           "{", "}", "?", "@", "&", "<", ">", "/", ":", "\n", "\t"
         ],
         "type": "text",
         "doc_value": True
       },
       "f2": {
         "doc_value": True,
         "type": "long"
       }
     },
     "storage": "pg",
     "ttl": 2,
     "index_mode": "v2",
     "line": {
       "caseSensitive": False,
       "token": [
         ",", " ", "\"", "\"", ";", "=", "(", ")", "[", "]", "{",
         "}", "?", "@", "&", "<", ">", "/", ":", "\n", "\t"
       ]
     }
   }
request = IndexConfig()
request.from_json(request_json)
res = client.create_index('project1', 'logstore1', request)
res.log_print()
```

更多索引样例，可以参考[这里](https://github.com/aliyun/aliyun-log-cli/tree/master/tests/index)，注意，使用SDK时，需要将`true/false`改成Python对应的`True/False`。

- 修改

  通过`update_index`修改日志库的索引

- 获取

  通过`get_index_config`获得日志库的索引配置

- 删除

  通过`delete_index`删除日志库的索引


### 其他操作

- 获取日志库主题列表

```python
from aliyun.log import ListTopicsRequest
request = ListTopicsRequest('project1', 'logstore1')
res = client.list_topic(request)
res.log_print()
```


## 日志消费

有三种方式消费日志:

1. 拉取数据(PullLog): 根据分区游标来消费日志: 需要指定分区(Shard)以及游标.
2. 日志库查询数据(GetLog): 通过索引查询来消费特定日志库日志: 需要指定日志库与日志时间以及(或)查询条件.
3. 项目统计查询数据(GetProjectLog): 通过索引查询来消费整个项目组日志: 需要指定日志时间以及(或)统计查询查询条件.
4. 实时消费(Consumer Group): 第一种的高级方式, 通过服务器支持的消费组, 来并发可靠的快速拉取日志.

### 游标操作

拉取数据需要传入游标和分区, 获取分区可以参考前面的`管理日志库分区(shard)`, 这里介绍游标操作.

- 获取开头游标

  获取日志库特定分区的最开头的游标.

```python
res = client.get_begin_cursor('project1', 'logstore1', shard_id=0)
print(res.get_cursor())
```

- 获取结尾游标

  获取日志库特定分区的结尾的游标.

```python
res = client.get_end_cursor('project1', 'logstore1', shard_id=0)
print(res.get_cursor())
```

- 获取特定时间的游标

  可以特定日志库分区的特定接受时间最接近的一个游标.

```python
res = client.get_cursor('project1', 'logstore1', shard_id=0, start_time="2018-1-1 10:10:10")
print(res.get_cursor())
```
  - 这里的`start_time`指的是服务器接受日志的时间. 也可以是`begin`或者`end`

- 获取游标时间

  获得特定日志库分区的某个游标说对应的服务器时间, 如果是结尾游标, 一般对应于服务器的的当前时间.

```python
res = client.get_begin_cursor('project1', 'logstore1', shard_id=0)
res = client.get_cursor_time('project1', 'logstore1', shard_id=0, cursor=res.get_cursor())
print(res.get_cursor_time())
```

- 获取游标时间

  获得特定日志库分区的某个游标的上一个游标所对应的服务器时间, 如果是开头游标, 则对应于服务器的的开头游标的时间.

```python
res = client.get_end_cursor('project1', 'logstore1', shard_id=0)
res = client.get_previous_cursor_time('project1', 'logstore1', shard_id=0, cursor=res.get_cursor())
print(res.get_cursor_time())
```


### 拉取(Pull)数据
根据游标获取数据, 需要传入分区.
下面例子消费分区`0`一个小时前收集到的数据.

```python
from time import time
res = client.get_cursor('project1', 'logstore1', shard_id=0, start_time=int(time())-3600)
res = client.pull_logs('project1', 'logstore1', shard_id=0, cursor=res.get_cursor())
res.log_print()

# 或者
it = client.pull_log('project1', 'logstore1', shard_id=0, from_time="2018-1-1 10:10:10", to_time="2018-1-1 10:20:10")
for res in it:
    res.log_print()

# 或者大并发直接下载在本地
it = client.pull_log('project1', 'logstore1', from_time="2018-1-1 10:10:10", to_time="2018-1-1 10:20:10", file_path="/data/dump_{}.data")
for res in it:
    res.log_print()
```

**注意：** 默认获取1000条, 可以通过参数`count`来调节. 也可以通过参数`end_cursor`来设定设定一个结束的游标.


### 获取(Get)日志库数据
消费特定日志库, 根据索引获取数据, 需要传入时间范围, 也可以传入查询语句.
下面的例子查询时间是过去一小时特定日志库的前100条日志.

```python
from time import time
from aliyun.log import GetLogsRequest
request = GetLogsRequest("project1", "logstore1", fromTime=int(time()-3600), toTime=int(time()), topic='', query="*", line=100, offset=0, reverse=False)
# 或者
request = GetLogsRequest("project1", "logstore1", fromTime="2018-1-1 10:10:10", toTime="2018-1-1 10:20:10", topic='', query="*", line=100, offset=0, reverse=False)


res = client.get_logs(request)
res.log_print()
```


也可以通过接口`get_log`来获取
```python
from time import time
res = client.get_log("project1", "logstore1", from_time=int(time()-3600), to_time=int(time()), size=-1)
# 或者
res = client.get_log("project1", "logstore1", from_time="2018-1-1 10:10:10", to_time="2018-1-1 10:20:10", size=-1)

res.log_print()
```

这里传入`size=-1`可以获取所有.


### 获取数据分布图
通过`get_histograms`来根据索引获取数据特定日志时间范围内的分布图.


### 获取(Get)项目组数据
跨日志库的对整个项目组查询, 根据索引获取数据, 需要传入时间范围和查询语句.
**注意：** 因为跨项目组传输数据可能非常大从而影响实际的性能, 推荐使用统计查询SQL来降低传输数据量.

下面的例子查询时间是一段时间内的项目组的几个日志库的日志数量.

```python
from aliyun.log import GetProjectLogsRequest

req = GetProjectLogsRequest("project1","select count(1) from logstore1, logstore2, logstore3 where __date__ >'2017-11-10 00:00:00' and __date__ < '2017-11-13 00:00:00'")
res = client.get_project_logs(req)
res.log_print();
```

注意：注意SQL中使用的字段需要在索引中配置好，例如上面例子中的`__date__`

## 实时消费
通过消费组(Consumer Group)可以获得可保障的自动扩展的日志消费服务.

### 高级接口
可以参考这这几篇实战文章与样例。
- [日志服务与SIEM（如Splunk）集成方案](https://aliyun-log-python-sdk.readthedocs.io/tutorials/tutorial_consumer_group_sync_log_to_splunk.html)
- [使用消费组实时分发数据](https://aliyun-log-python-sdk.readthedocs.io/tutorials/tutorial_consumer_group_dispatch_log.html)
- [使用消费组实时实时跨域监测多日志库数据](https://aliyun-log-python-sdk.readthedocs.io/tutorials/tutorial_consumer_group_monitor_multiple_logstores.html)
- 相关[样例代码](https://github.com/aliyun/aliyun-log-python-sdk/tree/master/tests/consumer_group_examples)

### 基础接口
高级接口已经对基础接口进行了封装. 个别情况下也可以通过基础接口进行一些特定的操作.

- 获取列表

  通过`list_consumer_group`h获得当前消费组列表.


- 创建

  通过`create_consumer_group`创建一个消费组.

- 更新

  通过`update_consumer_group`更新一个消费组, 例如延迟和消费顺序等.

- 删除

  通过`delete_consumer_group`删除一个消费组.

- 获取消费进度

  可以通过`get_check_point`获得消费组的消费检查点(Checkpoint), 来了解消费进度信息

- 更新消费进度

  消费者需要通过`update_check_point`来存储和更新消费检查点(Checkpoint)

## 报表管理
管理报表

- 获取列表

  通过`list_dashboard`获取报表的列表

- 创建报表

  通过`create_dashboard`创建一个报表. 传入的结构是一个字典对象，如下：
  
```python
{
  "charts": [
    {
      "display": {
        "displayName": "",
        "height": 5,
        "width": 5,
        "xAxis": [
          "province"
        ],
        "xPos": 0,
        "yAxis": [
          "pv"
        ],
        "yPos": 0
      },
      "search": {
        "end": "now",
        "logstore": "access-log",
        "query": "method:  GET  | select  ip_to_province(remote_addr) as province , count(1) as pv group by province order by pv desc ",
        "start": "-86400s",
        "topic": ""
      },
      "title": "map",
      "type": "map"
    },
    {
      "display": {
        "displayName": "",
        "height": 5,
        "width": 5,
        "xAxis": [
          "province"
        ],
        "xPos": 5,
        "yAxis": [
          "pv"
        ],
        "yPos": 0
      },
      "search": {
        "end": "now",
        "logstore": "access-log",
        "query": "method:  POST  | select  ip_to_province(remote_addr) as province , count(1) as pv group by province order by pv desc ",
        "start": "-86400s",
        "topic": ""
      },
      "title": "post_map",
      "type": "map"
    }
  ],
  "dashboardName": 'dashboard_1',
  "description": ""
}

```

- 获取报表

  通过`get_dashboard`获取一个报表的具体信息.

- 更新报表

  通过`update_dashboard`更新一个报表，传入的结构与创建一样。

- 删除报表

  通过`delete_dashboard`删除一个报表.

## 报警管理
管理报警

- 获取报警

  通过`list_alert`获取报警的列表

- 创建报警

  通过`create_alert`创建一个报警. 传入的结构是一个字典对象，如下：

```python
{
    "name": 'alert_1',
    "displayName": "Alert for testing",
    "description": "",
    "type": "Alert",
    "state": "Enabled",
    "schedule": {
        "type": "FixedRate",
        "interval": "5m",
    },
    "configuration": {
        "condition": "total >= 100",
        "dashboard": "dashboard-test",
        "queryList": [
            {
                "logStore": "test-logstore",
                "start": "-120s",
                "end": "now",
                "timeSpanType": "Custom",
                "chartTitle": "chart-test",
                "query": "* | select count(1) as total",
            }
        ],
        "notificationList": [
            {
                "type": "DingTalk",
                "serviceUri": "http://xxxx",
                "content": "Message",
            },
            {
                "type": "MessageCenter",
                "content": "Message",
            },
            {
                "type": "Email",
                "emailList": ["abc@test.com"],
                "content": "Email Message",
            },
            {
                "type": "SMS",
                "mobileList": ["132373830xx"],
                "content": "Cellphone message"
            }
        ],
        "muteUntil": 1544582517,
        "notifyThreshold": 1,
        "throttling": "5m",
    }
}
```

- 获取报警

  通过`get_alert`获取一个报警的具体信息.

- 更新报警

  通过`update_alert`更新一个报警，传入的结构与创建一样。

- 删除报警

  通过`delete_alert`删除一个报警.

## 快速查询管理
管理快速查询

- 获取快速查询

  通过`list_savedsearch`获取快速查询的列表

- 创建快速查询

  通过`create_savedsearch`创建一个快速查询. 传入的结构是一个字典对象，如下：
  
```python
{
    "logstore": "test2",
    "savedsearchName": 'search_1',
    "searchQuery": "boy | select sex, count() as Count group by sex",
    "topic": ""
}
```

- 获取快速查询

  通过`get_savedsearch`获取一个快速查询的具体信息.

- 更新快速查询

  通过`update_savedsearch`更新一个快速查询，传入的结构与创建一样。

- 删除快速查询

  通过`delete_savedsearch`删除一个快速查询.

## 外部存储管理
管理外部存储, 参考[文章](https://help.aliyun.com/document_detail/70479.html)

- 获取外部存储列表

  通过`list_external_store`获取快速查询的列表

- 创建外部存储

  通过`create_external_store`创建一个快速查询. 传入的结构是一个字典对象，如下：
  
```python
{
	"externalStoreName": "rds_store4",
	"storeType": "rds-vpc",
	"parameter": {
		"vpc-id": "vpc-m5eq4irc1pucpk85frr5j",
		"instance-id": "i-m5eeo2whsnfg4kzq54ah",
		"host": "1.2.3.4",
		"port": "3306",
		"username": "root",
		"password": "123",
		"db": "meta",
		"table": "join_meta",
		"region": "cn-qingdao"
	}
}
```

- 获取外部存储

  通过`get_external_store`获取一个快速查询的具体信息.

- 更新外部存储

  通过`update_external_store`更新一个快速查询，传入的结构与创建一样。

- 删除外部存储

  通过`delete_external_store`删除一个快速查询.



## 投递管理
投递的配置一般称为Job, 包含了投递的具体配置以及调度日程安排. 而某一个具体时间的运行实例称为Task.

- 获取配置列表

  通过`list_shipper`获取投递配置的列表

- 创建配置

  通过`create_shipper`创建一个投递配置.

- 获取配置

  通过`get_shipper_config`获取一个投递配置的具体信息.

- 更新配置

  通过`update_shipper`更新一个投递配置.

- 删除配置

  通过`delete_shipper`删除一个投递配置.

- 获取运行实例列表

  通过`get_shipper_tasks`获取投递运行实例.

- 重试运行实例

  通过`retry_shipper_tasks`重试某一个运行实例.
  
## Elasticsearch 数据迁移
用于将 Elasticsearch 中的数据迁移至日志服务。

- 将 hosts 为 `localhost:9200` 的 Elasticsearch 中的所有文档导入日志服务的项目 `project1` 中。
```
migration_manager = MigrationManager(hosts="localhost:9200",      
                                     endpoint=endpoint,
                                     project_name="project1",
                                     access_key_id=access_key_id,
                                     access_key=access_key)
migration_manager.migrate()
```

- 指定将 Elasticsearch 中索引名以 `myindex_` 开头的数据写入日志库 `logstore1`，将索引 `index1,index2` 中的数据写入日志库 `logstore2` 中。
```
migration_manager = MigrationManager(hosts="localhost:9200,other_host:9200",      
                                     endpoint=endpoint,
                                     project_name="project1",
                                     access_key_id=access_key_id,
                                     access_key=access_key,
				     logstore_index_mappings='{"logstore1": "myindex_*", "logstore2": "index1,index2"}}')
migration_manager.migrate()
```

- 使用参数 query 指定从 Elasticsearch 中抓取 `title` 字段等于 `python` 的文档，并使用文档中的字段 `date1` 作为日志的 time 字段。
```
migration_manager = MigrationManager(hosts="localhost:9200",      
                                     endpoint=endpoint,
                                     project_name="project1",
                                     access_key_id=access_key_id,
                                     access_key=access_key,
				     query='{"query": {"match": {"title": "python"}}}',
				     time_reference="date1")
migration_manager.migrate()
```

## 最佳实践
- [日志服务与SIEM（如Splunk）集成方案](https://aliyun-log-python-sdk.readthedocs.io/tutorials/tutorial_consumer_group_sync_log_to_splunk.html)
- [使用消费组实时分发数据](https://aliyun-log-python-sdk.readthedocs.io/tutorials/tutorial_consumer_group_dispatch_log.html)
- [使用消费组实时实时跨域监测多日志库数据](https://aliyun-log-python-sdk.readthedocs.io/tutorials/tutorial_consumer_group_monitor_multiple_logstores.html)
- [使用Log Handler自动上传Python日志](https://aliyun-log-python-sdk.readthedocs.io/tutorials/tutorial_logging_handler.html)
- [Log Handler自动解析KV格式的日志](https://aliyun-log-python-sdk.readthedocs.io/tutorials/tutorial_logging_handler_kv.html)
- [Log Handler自动解析JSON格式的日志](https://aliyun-log-python-sdk.readthedocs.io/tutorials/tutorial_logging_handler_json.html)
- [Elasticsearch 数据迁移](https://github.com/aliyun/aliyun-log-python-sdk/blob/master/doc/tutorials/tutorial_es_migration.md)

## 其他资源：

1. 日志服务产品介绍：http://www.aliyun.com/product/sls/
2. 日志服务产品文档：https://help.aliyun.com/product/28958.html
3. 其他问题请提工单

