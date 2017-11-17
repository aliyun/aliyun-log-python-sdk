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

### 具体功能

1. 封装Rest API。
2. 实现API请求的数字签名
3. 实现API的Protocol Buffer格式发送日志
4. 支持API定义的数据压缩方式
5. 实现API查询数据和批量消费数据
6. 使用异常统一处理错误
7. 提供消费组高级API

### 支持Python版本

1. Python 2.6
2. Python 2.7
3. Python 3.3
4. Python 3.4
5. Python 3.5
6. Python 3.6
7. Pypy2
8. Pypy3


### 支持API版本

1. Log Service API 0.6

## 安装
```shell
pip install -U aliyun-log-python-sdk
```

## 代码示例
- [代码示例](https://github.com/aliyun/aliyun-log-python-sdk/tree/master/tests)


## 配置SDK
参考[SDK配置](https://help.aliyun.com/document_detail/29064.html?spm=5176.doc29068.2.8.SWJhYZ)
获得访问秘钥的ID和Key以及访问入口Endpoint, 构建一个LogClient的客户端.

```python
from aliyun.log import LogClient

# “华东 1 (杭州)” Region 的日志服务入口。
endpoint = 'cn-hangzhou.sls.aliyuncs.com'
# 用户访问秘钥对中的 AccessKeyId。
accessKeyId = 'ABCDEFGHIJKLJMN'
# 用户访问秘钥对中的 AccessKeySecret。
accessKey = 'OPQRSTUVWXYZ'

client = LogClient(endpoint, accessKeyId, accessKey)
# 使用client的方法来操作日志服务
```

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
  res = client.create_project("new_project", "a simple project"")
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
  from aliyun.log import LogtailConfigHelper as helper
  config_detail_json = {
        "config_name": "config_name1",
        "logstore_name": "logstore1",
        "file_pattern": "file_pattern",
        "time_format": "time_format",
        "log_path": "/log_path",
        "endpoint": "endpoint",
        "log_parse_regex": "xxx ([\\w\\-]+\\s[\\d\\:]+)\\s+(.*)",
        "log_begin_regex": "xxx.*",
        "reg_keys": [
          "time",
          "value"
        ],
        "topic_format": "none",
        "filter_keys": [
          "time",
          "value"
        ],
        "filter_keys_reg": [
          "time",
          "value"
        ],
        "logSample": "xxx 2017-11-11 11:11:11 hello alicloud."
      }
  request = helper.generate_common_reg_log_config(config_detail)
  res = client.create_logtail_config('project1', request)
  res.log_print()
  ```

  **注意：**
  - 创建的配置的名字`config_name`和关联的日志库名字`logstore_name`都是放在传入的`request`中.
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
	         "caseSensitive": false,
	         "token": [
	           ",", " ", "\"", "\"", ";", "=",  "(", ")", "[", "]",
	           "{", "}", "?", "@", "&", "<", ">", "/", ":", "\n", "\t"
	         ],
	         "type": "text",
	         "doc_value": true
	       },
	       "f2": {
	         "doc_value": true,
	         "type": "long"
	       }
	     },
	     "storage": "pg",
	     "ttl": 2,
	     "index_mode": "v2",
	     "line": {
	       "caseSensitive": false,
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
2. 查询数据(GetLog): 通过索引查询来消费日志: 需要指定日志时间以及(或)查询条件.
3. 实时消费(Consumer Group): 第一种的高级方式, 通过服务器支持的消费组, 来并发可靠的快速拉取日志.

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
  res = client.get_cursor('project1', 'logstore1', shard_id=0, start_time=1510837205)
  print(res.get_cursor())
  ```

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
```

**注意：** 默认获取1000条, 可以通过参数`count`来调节. 也可以通过参数`end_cursor`来设定设定一个结束的游标.


### 获取(Get)数据
根据索引获取数据, 需要传入时间范围, 也可以传入查询语句.
下面的例子查询时间是过去一小时的日志.

```python
from time import time
from aliyun.log import GetLogsRequest
request_json = {
  "project": "project1",
  "logstore": "logstore1",
  "topic": "",
  "toTime": str(int(time())),
  "offset": "0",
  "query": "*",
  "line": "100",
  "fromTime": str(int(time()-3600)),
  "reverse": "false"
}
request = GetLogsRequest("project1", "logstore1", fromTime=int(time()-3600), toTime=int(time()), topic='', query="*", line=1000, offset=0, reverse=False)
res = client.get_logs(request)
res.log_print()
```


### 获取数据分布图
通过`get_histograms`来根据索引获取数据特定日志时间范围内的分布图.


## 实时消费
通过消费组(Consumer Group)可以获得可保障的自动扩展的日志消费服务.

### 高级接口
1. 构建消费逻辑

	继承类`ConsumerProcessorBase`重写方法`initialize`, `process`和`shutdown`定义特定逻辑.

	```python
	from aliyun.log.consumer import ConsumerProcessorBase

	class SampleConsumer(ConsumerProcessorBase):
	    def initialize(self, shard):
	        pass

	    def process(self, log_groups, check_point_tracker):
	        for log_group in log_groups.LogGroups:
	            items = []
	            for log in log_group.Logs:
	                item = dict()
	                item['time'] = log.Time
	                for content in log.Contents:
	                    item[content.Key] = content.Value
	                items.append(item)
	            log_items = dict()
	            log_items['topic'] = log_group.Topic
	            log_items['source'] = log_group.Source
	            log_items['logs'] = items

	            # 打印日志
	            print(log_items)

	        check_point_tracker.save_check_point(True)


	    def shutdown(self, check_point_tracker):
	        check_point_tracker.save_check_point(True)

	```

2. 构建消费工作者

	这里在同一消费组下准备2个消费者的配置项:

	```python
	from aliyun.log.consumer import LogHubConfig, CursorPosition

	# 准备配置项
	option1 = LogHubConfig(endpoint, access_id, access_key, "project1", "logstore1", "consume_group1",
	                       "consumer A", CursorPosition.BEGIN_CURSOR)
	option2 = LogHubConfig(endpoint, access_id, access_key, "project1", "logstore1", "consume_group1",
	                       "consumer B", CursorPosition.BEGIN_CURSOR)
	```

	构建两个工作者:

	```python
	worker1 = ConsumerWorker(SampleConsumer, option1)
	worker2 = ConsumerWorker(SampleConsumer, option2)
	```

3. 启动关闭工作者


	```python
	# 启动
	worker1.start()
	worker2.start()

	time.sleep(60)

	# 关闭
	worker1.shutdown()
	worker2.shutdown()
	```

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


## 其他资源：

1. 日志服务产品介绍：http://www.aliyun.com/product/sls/
2. 日志服务产品文档：https://help.aliyun.com/product/28958.html
3. 其他问题请提工单
