# 日志服务与SIEM（如Splunk）集成方案

# 背景信息
## 目标
本文主要介绍如何让阿里云日志服务与您的SIEM方案(如Splunk)对接, 以便确保阿里云上的所有法规、审计、与其他相关日志能够导入到您的安全运维中心（SOC）中。

## 名词解释
**LOG（SLS）** - 阿里云日志服务，简写SLS表示（Simple Log Service）。
**SIEM** - 安全信息与事件管理系统（Security Information and Event Management）,如Splunk, QRadar等。
**Splunk HEC** - Splunk的Http事件接收器（Splunk Http Event Collector）, 一个 HTTP(s)接口，用于接收日志。


## 审计相关日志
安全运维团队一般对阿里云相关的审计日志感兴趣，如下列出所有存在于所有目前在日志服务中可用的相关日志（但不限于）：
![image](https://yqfile.alicdn.com/97a7a17dc30033ef97e1bbb43433c9c13fe01691.png)
* Regions化 - 时刻更新，请以最新的产品文档为准。

## 阿里云日志服务
阿里云的日志服务（log service）是针对日志类数据的一站式服务，无需开发就能快捷完成海量日志数据的采集、消费、投递以及查询分析等功能，提升运维、运营效率。日志服务主要包括 实时采集与消费、数据投递、查询与实时分析 等功能，适用于从实时监控到数据仓库的各种开发、运维、运营与安全场景：
![image](https://yqfile.alicdn.com/596eb88f43771caf1017dc2d63599f930178509b.png)

目前，以上各个阿里云产品已经与[日志服务](https://www.aliyun.com/product/sls/)打通，提供近实时的日志自动采集存储、并提供基于日志服务的查询分析、报表报警、下游计算对接与投递的能力。

![image](https://yqfile.alicdn.com/9589741de97db53e735dcdb260d7354ab33c7caf.png)


# 集成方案建议
## 概念
**项目（Project）**
项目（Project）是日志服务中的资源管理单元，用于资源隔离和控制。您可以通过项目来管理某一个应用的所有日志及相关的日志源。它管理着用户的所有日志库（Logstore），采集日志的机器配置等信息，同时它也是用户访问日志服务资源的入口。


**日志库（Logstore）**
日志库（Logstore）是日志服务中日志数据的收集、存储和查询单元。每个日志库隶属于一个项目，且每个项目可以创建多个日志库。

**分区（Shard）**
每个日志库分若干个分区（Shard），每个分区由MD5左闭右开区间组成，每个区间范围不会相互覆盖，并且所有的区间的范围是MD5整个取值范围。

**服务入口（Endpoint）**
日志服务入口是访问一个项目（Project）及其内部日志数据的 URL。它和 Project 所在的阿里云区域（Region）及 Project 名称相关。
https://help.aliyun.com/document_detail/29008.html

**访问秘钥（AccessKey）**
阿里云访问秘钥是阿里云为用户使用 API（非控制台）来访问其云资源设计的“安全口令”。您可以用它来签名 API 请求内容以通过服务端的安全验证。
https://help.aliyun.com/document_detail/29009.html


## 假设
这里假设您的SIEM（如Splunk）位于组织内部环境（on-premise）中，而不是云端。为了安全考虑，没有任何端口开放让外界环境来访问此SIEM。

## 概览
推荐使用SLS消费组构建程序来从SLS进行实时消费，然后通过Splunk API（HEC）来发送日志给Splunk。

![image](https://yqfile.alicdn.com/f8af7f508519a23e6cbc0947d29e51199b1bcb45.png)


# 使用消费组编程

协同消费库（Consumer Library）是对日志服务中日志进行消费的高级模式，提供了消费组（ConsumerGroup）的概念对消费端进行抽象和管理，和直接使用SDK进行数据读取的区别在于，用户无需关心日志服务的实现细节，只需要专注于业务逻辑，另外，消费者之间的负载均衡、failover等用户也都无需关心。

Spark Streaming、Storm 以及Flink Connector都以Consumer Library作为基础实现。


## 基本概念
**消费组（Consumer Group）** - 一个消费组由多个消费者构成，同一个消费组下面的消费者共同消费一个logstore中的数据，消费者之间不会重复消费数据。
**消费者（Consumer）** - 消费组的构成单元，实际承担消费任务，同一个消费组下面的消费者名称必须不同。

在日志服务中，一个logstore下面会有多个shard，协同消费库的功能就是将shard分配给一个消费组下面的消费者，分配方式遵循以下原则：
- 每个shard只会分配到一个消费者。
- 一个消费者可以同时拥有多个shard。
新的消费者加入一个消费组，这个消费组下面的shard从属关系会调整，以达到消费负载均衡的目的，但是上面的分配原则不会变，分配过程对用户透明。

协同消费库的另一个功能是保存checkpoint，方便程序故障恢复时能接着从断点继续消费，从而保证数据不会被重复消费。

## 部署建议
### 硬件建议
**硬件参数：**
需要一台机器运行程序，安装一个Linux（如Ubuntu x64），推荐硬件参数如下：
- 2.0+ GHZ X 8核
- 16GB 内存，推荐32GB
- 1 Gbps网卡
- 至少2GB可用磁盘空间，建议10GB以上

**网络参数：**
从组织内的环境到阿里云的带宽应该大于数据在阿里云端产生的速度，否则日志无法实时消费。假设数据产生一般速度均匀，峰值在2倍左右，每天100TB原始日志。5倍压缩的场景下，推荐带宽应该在4MB/s（32Mbps）左右。

## 使用(Python)
这里我们描述用Python使用消费组进行编程。对于Java语言用法，可以参考这篇[文章](https://www.alibabacloud.com/help/doc-detail/28998.htm).

注意：本篇文章的代码可能会更新，最新版本在这里可以找到：[Github样例](https://github.com/aliyun/aliyun-log-python-sdk/blob/master/tests/consumer_group_examples/sync_data_to_splunk.py).

### 安装
**环境**
1. 强烈推荐[PyPy3](https://pypy.org/download.html)来运行本程序，而不是使用标准CPython解释器。
2. 日志服务的Python SDK可以如下安装：
```shell
pypy3 -m pip install aliyun-log-python-sdk -U
```
更多SLS Python SDK的使用手册，可以参考[这里](https://aliyun-log-python-sdk.readthedocs.io/README_CN.html)


### 程序配置
如下展示如何配置程序：
1. 配置程序日志文件，以便后续测试或者诊断可能的问题。
2. 基本的日志服务连接与消费组的配置选项。
3. 消费组的一些高级选项（性能调参，不推荐修改）。
4. SIEM（Splunk）的相关参数与选项。

请仔细阅读代码中相关注释并根据需要调整选项：

```python
#encoding: utf8
import os
import logging
from logging.handlers import RotatingFileHandler

root = logging.getLogger()
handler = RotatingFileHandler("{0}_{1}.log".format(os.path.basename(__file__), current_process().pid), maxBytes=100*1024*1024, backupCount=5)
handler.setFormatter(logging.Formatter(fmt='[%(asctime)s] - [%(threadName)s] - {%(module)s:%(funcName)s:%(lineno)d} %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
root.setLevel(logging.INFO)
root.addHandler(handler)
root.addHandler(logging.StreamHandler())

logger = logging.getLogger(__name__)

def get_option():
    ##########################
    # 基本选项
    ##########################

    # 从环境变量中加载SLS参数与选项
    endpoint = os.environ.get('SLS_ENDPOINT', '')
    accessKeyId = os.environ.get('SLS_AK_ID', '')
    accessKey = os.environ.get('SLS_AK_KEY', '')
    project = os.environ.get('SLS_PROJECT', '')
    logstore = os.environ.get('SLS_LOGSTORE', '')
    consumer_group = os.environ.get('SLS_CG', '')

    # 消费的起点。这个参数在第一次跑程序的时候有效，后续再次运行将从上一次消费的保存点继续。
    # 可以使”begin“，”end“，或者特定的ISO时间格式。
    cursor_start_time = "2018-12-26 0:0:0"

    ##########################
    # 一些高级选项
    ##########################

    # 一般不要修改消费者名，尤其是需要并发跑时
    consumer_name = "{0}-{1}".format(consumer_group, current_process().pid)

    # 心跳时长，当服务器在2倍时间内没有收到特定Shard的心跳报告时，服务器会认为对应消费者离线并重新调配任务。
    # 所以当网络不是特别好的时候，不要调整的特别小。
    heartbeat_interval = 20

    # 消费数据的最大间隔，如果数据生成的速度很快，并不需要调整这个参数。
    data_fetch_interval = 1

    # 构建一个消费组和消费者
    option = LogHubConfig(endpoint, accessKeyId, accessKey, project, logstore, consumer_group, consumer_name,
                          cursor_position=CursorPosition.SPECIAL_TIMER_CURSOR,
                          cursor_start_time=cursor_start_time,
                          heartbeat_interval=heartbeat_interval,
                          data_fetch_interval=data_fetch_interval)

    # Splunk选项
    settings = {
                "host": "10.1.2.3",
                "port": 80,
                "token": "a023nsdu123123123",
                'https': False,              # 可选, bool
                'timeout': 120,             # 可选, int
                'ssl_verify': True,         # 可选, bool
                "sourcetype": "",            # 可选, sourcetype
                "index": "",                # 可选, index
                "source": "",               # 可选, source
            }

    return option, settings
```


### 数据消费与转发
如下代码展示如何从SLS拿到数据后转发给Splunk。


```python
from aliyun.log.consumer import *
from aliyun.log.pulllog_response import PullLogResponse
from multiprocessing import current_process
import time
import json
import socket
import requests

class SyncData(ConsumerProcessorBase):
    """
    这个消费者从SLS消费数据并发送给Splunk
    """
    def __init__(self, splunk_setting):
	    """初始化并验证Splunk连通性"""
        super(SyncData, self).__init__()

        assert splunk_setting, ValueError("You need to configure settings of remote target")
        assert isinstance(splunk_setting, dict), ValueError("The settings should be dict to include necessary address and confidentials.")

        self.option = splunk_setting
        self.timeout = self.option.get("timeout", 120)

        # 测试Splunk连通性
        s = socket.socket()
        s.settimeout(self.timeout)
        s.connect((self.option["host"], self.option['port']))

        self.r = requests.session()
        self.r.max_redirects = 1
        self.r.verify = self.option.get("ssl_verify", True)
        self.r.headers['Authorization'] = "Splunk {}".format(self.option['token'])
        self.url = "{0}://{1}:{2}/services/collector/event".format("http" if not self.option.get('https') else "https", self.option['host'], self.option['port'])

        self.default_fields = {}
        if self.option.get("sourcetype"):
            self.default_fields['sourcetype'] = self.option.get("sourcetype")
        if self.option.get("source"):
            self.default_fields['source'] = self.option.get("source")
        if self.option.get("index"):
            self.default_fields['index'] = self.option.get("index")

    def process(self, log_groups, check_point_tracker):
        logs = PullLogResponse.loggroups_to_flattern_list(log_groups, time_as_str=True, decode_bytes=True)
        logger.info("Get data from shard {0}, log count: {1}".format(self.shard_id, len(logs)))
        for log in logs:
            # 发送数据到Splunk
            # 如下代码只是一个样例（注意：所有字符串都是unicode）
            #    Python2: {u"__time__": u"12312312", u"__topic__": u"topic", u"field1": u"value1", u"field2": u"value2"}
            #    Python3: {"__time__": "12312312", "__topic__": "topic", "field1": "value1", "field2": "value2"}
            event = {}
            event.update(self.default_fields)
            if log.get(u"__topic__") == 'audit_log':
                # suppose we only care about audit log
                event['time'] = log[u'__time__']
                event['fields'] = {}
                del log['__time__']
                event['fields'].update(log)

                data = json.dumps(event, sort_keys=True)

                try:
                    req = self.r.post(self.url, data=data, timeout=self.timeout)
                    req.raise_for_status()
                except Exception as err:
                    logger.debug("Failed to connect to remote Splunk server ({0}). Exception: {1}", self.url, err)

                    # TODO: 根据需要，添加一些重试或者报告的逻辑

        logger.info("Complete send data to remote")

        self.save_checkpoint(check_point_tracker)

```


### 主逻辑
如下代码展示主程序控制逻辑：

```python
def main():
    option, settings = get_monitor_option()

    logger.info("*** start to consume data...")
    worker = ConsumerWorker(SyncData, option, args=(settings,) )
    worker.start(join=True)

if __name__ == '__main__':
    main()
```

### 启动
假设程序命名为"sync_data.py"，可以如下启动：

```shell
export SLS_ENDPOINT=<Endpoint of your region>
export SLS_AK_ID=<YOUR AK ID>
export SLS_AK_KEY=<YOUR AK KEY>
export SLS_PROJECT=<SLS Project Name>
export SLS_LOGSTORE=<SLS Logstore Name>
export SLS_CG=<消费组名，可以简单命名为"syc_data">

pypy3 sync_data.py
```

**限制与约束**
每一个日志库（logstore）最多可以配置10个消费组，如果遇到错误`ConsumerGroupQuotaExceed`则表示遇到限制，建议在控制台端删除一些不用的消费组。

## 检测
- [在控制台查看消费组状态](https://help.aliyun.com/document_detail/43998.html)
- [通过云监控查看消费组延迟，并配置报警](https://help.aliyun.com/document_detail/55912.html)

## 性能考虑
### 启动多个消费者
基于消费组的程序可以直接启动多次以便达到并发作用：

```shell
nohup pypy3 sync_data.py &
nohup pypy3 sync_data.py &
nohup pypy3 sync_data.py &
...
```

**注意:**
所有消费者使用了同一个消费组的名字和不同的消费者名字（因为消费者名以进程ID为后缀）。
因为一个分区（Shard）只能被一个消费者消费，假设一个日志库有10个分区，那么最多有10个消费者同时消费。

### Https
如果服务入口（endpoint）配置为`https://`前缀，如`https://cn-beijing.log.aliyuncs.com`，程序与SLS的连接将自动使用HTTPS加密。

服务器证书`*.aliyuncs.com`是GlobalSign签发，默认大多数Linux/Windows的机器会自动信任此证书。如果某些特殊情况，机器不信任此证书，可以参考[这里](https://success.outsystems.com/Support/Enterprise_Customers/Installation/Install_a_trusted_root_CA__or_self-signed_certificate)下载并安装此证书。

### 性能吞吐
基于测试，在没有带宽限制、接收端速率限制（如Splunk端）的情况下，以推进硬件用`pypy3`运行上述样例，单个消费者占用大约`10%的单核CPU`下可以消费达到`5 MB/s`原始日志的速率。因此，理论上可以达到`50 MB/s`原始日志`每个CPU核`，也就是`每个CPU核每天可以消费4TB原始日志`。

**注意:** 这个数据依赖带宽、硬件参数和SIEM接收端（如Splunk）是否能够较快接收数据。

### 高可用性
消费组会将检测点（check-point）保存在服务器端，当一个消费者停止，另外一个消费者将自动接管并从断点继续消费。

可以在不同机器上启动消费者，这样当一台机器停止或者损坏的清下，其他机器上的消费者可以自动接管并从断点进行消费。

理论上，为了备用，也可以启动大于shard数量的消费者。


## 更多案例
- 日志服务Python消费组实战（一）：[日志服务与SIEM（如Splunk）集成实战](https://yq.aliyun.com/articles/684035)
- 日志服务Python消费组实战（二）：[实时日志分发](https://yq.aliyun.com/articles/684081)
- 日志服务Python消费组实战（三）：[实时跨域监测多日志库数据](https://yq.aliyun.com/articles/684108)
- 日志服务Python消费组实战（三）：[日志服务与SIEM（集成实战(二)：syslog篇](https://yq.aliyun.com/articles/684108)
- [本文Github样例](https://github.com/aliyun/aliyun-log-python-sdk/blob/master/tests/consumer_group_examples/sync_data_to_splunk.py)
