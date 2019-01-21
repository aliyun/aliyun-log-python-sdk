# 使用消费组实时分发数据

## 场景目标
使用日志服务的Web-tracking、logtail（文件极简）、syslog等收集上来的日志经常存在各种各样的格式，我们需要针对特定的日志（例如topic）进行一定的分发到特定的logstore中处理和索引，本文主要介绍如何使用消费组实时分发日志到不通的目标日志库中。并且利用消费组的特定，达到自动平衡、负载均衡和高可用性。

![image](https://yqfile.alicdn.com/cdd946a301ff55389b6f03559de059b6dc3b4b81.png)

## 基本概念
协同消费库（Consumer Library）是对日志服务中日志进行消费的高级模式，提供了消费组（ConsumerGroup）的概念对消费端进行抽象和管理，和直接使用SDK进行数据读取的区别在于，用户无需关心日志服务的实现细节，只需要专注于业务逻辑，另外，消费者之间的负载均衡、failover等用户也都无需关心。

**消费组（Consumer Group）** - 一个消费组由多个消费者构成，同一个消费组下面的消费者共同消费一个logstore中的数据，消费者之间不会重复消费数据。
**消费者（Consumer）** - 消费组的构成单元，实际承担消费任务，同一个消费组下面的消费者名称必须不同。

在日志服务中，一个logstore下面会有多个shard，协同消费库的功能就是将shard分配给一个消费组下面的消费者，分配方式遵循以下原则：
- 每个shard只会分配到一个消费者。
- 一个消费者可以同时拥有多个shard。
新的消费者加入一个消费组，这个消费组下面的shard从属关系会调整，以达到消费负载均衡的目的，但是上面的分配原则不会变，分配过程对用户透明。

协同消费库的另一个功能是保存checkpoint，方便程序故障恢复时能接着从断点继续消费，从而保证数据不会被重复消费。

## 使用消费组进行实时分发

这里我们描述用Python使用消费组进行编程，实时根据数据的topic进行分发。
注意：本篇文章的相关代码可能会更新，最新版本在这里可以找到：[Github样例](https://github.com/aliyun/aliyun-log-python-sdk/blob/master/tests/consumer_group_examples/copy_data_to_logstore.py).

![image](https://yqfile.alicdn.com/2271e718feaad160eff7c49779caefb139449d48.png)


### 安装
**环境**
1. 建议程序运行在源日志库同Region下的ECS上，并使用[局域网服务入口](https://help.aliyun.com/document_detail/29008.html)，这样好处是网络速度最快，其次是读取没有外网费用产生。
2. 强烈推荐[PyPy3](https://pypy.org/download.html)来运行本程序，而不是使用标准CPython解释器。
3. 日志服务的Python SDK可以如下安装：
```shell
pypy3 -m pip install aliyun-log-python-sdk -U
```
更多SLS Python SDK的使用手册，可以参考[这里](https://aliyun-log-python-sdk.readthedocs.io/README_CN.html)

### 程序配置
如下展示如何配置程序：
1. 配置程序日志文件，以便后续测试或者诊断可能的问题（跳过，具体参考样例）。
2. 基本的日志服务连接与消费组的配置选项。
3. 目标Logstore的一些连接信息

请仔细阅读代码中相关注释并根据需要调整选项：

```python
#encoding: utf8
def get_option():
    ##########################
    # 基本选项
    ##########################

    # 从环境变量中加载SLS参数与选项，根据需要可以配置多个目标
    accessKeyId = os.environ.get('SLS_AK_ID', '')
    accessKey = os.environ.get('SLS_AK_KEY', '')
    endpoint = os.environ.get('SLS_ENDPOINT', '')
    project = os.environ.get('SLS_PROJECT', '')
    logstore = os.environ.get('SLS_LOGSTORE', '')
    to_endpoint = os.environ.get('SLS_ENDPOINT_TO', endpoint)
    to_project = os.environ.get('SLS_PROJECT_TO', project)
    to_logstore1 = os.environ.get('SLS_LOGSTORE_TO1', '')
    to_logstore2 = os.environ.get('SLS_LOGSTORE_TO2', '')
    to_logstore3 = os.environ.get('SLS_LOGSTORE_TO3', '')
    consumer_group = os.environ.get('SLS_CG', '')

    # 消费的起点。这个参数在第一次跑程序的时候有效，后续再次运行将从上一次消费的保存点继续。
    # 可以使”begin“，”end“，或者特定的ISO时间格式。
    cursor_start_time = "2018-12-26 0:0:0"

    # 一般不要修改消费者名，尤其是需要并发跑时
    consumer_name = "{0}-{1}".format(consumer_group, current_process().pid)

    # 构建一个消费组和消费者
    option = LogHubConfig(endpoint, accessKeyId, accessKey, project, logstore, consumer_group, consumer_name, cursor_position=CursorPosition.SPECIAL_TIMER_CURSOR, cursor_start_time=cursor_start_time)

    # bind put_log_raw which is faster
    to_client = LogClient(to_endpoint, accessKeyId, accessKey)
    put_method1 = partial(to_client.put_log_raw, project=to_project, logstore=to_logstore1)
    put_method2 = partial(to_client.put_log_raw, project=to_project, logstore=to_logstore2)
    put_method3 = partial(to_client.put_log_raw, project=to_project, logstore=to_logstore3)

    return option, {u'ngnix': put_method1, u'sql_audit': put_method2, u'click': put_method3}
```

注意，这里使用了`functools.partial`对`put_log_raw`进行绑定，以便后续调用方便。


### 数据消费与分发
如下代码展示如何从SLS拿到数据后根据`topic`进行转发。

```python
if __name__ == '__main__':
    option, put_methods = get_copy_option()

    def copy_data(shard_id, log_groups):
        for log_group in log_groups.LogGroups:
            # update topic
            if log_group.Topic in put_methods:
		        put_methods[log_group.Topic](log_group=log_group)

    logger.info("*** start to consume data...")
    worker = ConsumerWorker(ConsumerProcessorAdaptor, option, args=(copy_data, ))
    worker.start(join=True)
```

### 启动
假设程序命名为"dispatch_data.py"，可以如下启动：

```shell
export SLS_ENDPOINT=<Endpoint of your region>
export SLS_AK_ID=<YOUR AK ID>
export SLS_AK_KEY=<YOUR AK KEY>
export SLS_PROJECT=<SLS Project Name>
export SLS_LOGSTORE=<SLS Logstore Name>
export SLS_LOGSTORE_TO1=<SLS To Logstore1 Name>
export SLS_LOGSTORE_TO1=<SLS To Logstore2 Name>
export SLS_LOGSTORE_TO1=<SLS To Logstore3 Name>
export SLS_CG=<消费组名，可以简单命名为"dispatch_data">

pypy3 dispatch_data.py
```
## 性能考虑
### 启动多个消费者
基于消费组的程序可以直接启动多次以便达到并发作用：

```shell
nohup pypy3 dispatch_data.py &
nohup pypy3 dispatch_data.py &
nohup pypy3 dispatch_data.py &
...
```

**注意:**
所有消费者使用了同一个消费组的名字和不同的消费者名字（因为消费者名以进程ID为后缀）。
因为一个分区（Shard）只能被一个消费者消费，假设一个日志库有10个分区，那么最多有10个消费者同时消费。

### 性能吞吐
基于测试，在没有带宽限制、接收端速率限制（如Splunk端）的情况下，以推进硬件用`pypy3`运行上述样例，单个消费者占用大约`10%的单核CPU`下可以消费达到`5 MB/s`原始日志的速率。因此，理论上可以达到`50 MB/s`原始日志`每个CPU核`，也就是`每个CPU核每天可以消费4TB原始日志`。

**注意:** 这个数据依赖带宽、硬件参数和SIEM接收端（如Splunk）是否能够较快接收数据。

### 高可用性
消费组会将检测点（check-point）保存在服务器端，当一个消费者停止，另外一个消费者将自动接管并从断点继续消费。

可以在不同机器上启动消费者，这样当一台机器停止或者损坏的清下，其他机器上的消费者可以自动接管并从断点进行消费。

理论上，为了备用，也可以启动大于shard数量的消费者。

## 其他

### 限制与约束
每一个日志库（logstore）最多可以配置10个消费组，如果遇到错误`ConsumerGroupQuotaExceed`则表示遇到限制，建议在控制台端删除一些不用的消费组。

### 监测
- [在控制台查看消费组状态](https://help.aliyun.com/document_detail/43998.html)
- [通过云监控查看消费组延迟，并配置报警](https://help.aliyun.com/document_detail/55912.html)

### Https
如果服务入口（endpoint）配置为`https://`前缀，如`https://cn-beijing.log.aliyuncs.com`，程序与SLS的连接将自动使用HTTPS加密。

服务器证书`*.aliyuncs.com`是GlobalSign签发，默认大多数Linux/Windows的机器会自动信任此证书。如果某些特殊情况，机器不信任此证书，可以参考[这里](https://success.outsystems.com/Support/Enterprise_Customers/Installation/Install_a_trusted_root_CA__or_self-signed_certificate)下载并安装此证书。

## 更多案例
- 日志服务Python消费组实战（一）：[日志服务与SIEM（如Splunk）集成实战](https://yq.aliyun.com/articles/684035)
- 日志服务Python消费组实战（二）：[实时日志分发](https://yq.aliyun.com/articles/684081)
- 日志服务Python消费组实战（三）：[实时跨域监测多日志库数据](https://yq.aliyun.com/articles/684108)
- 日志服务Python消费组实战（三）：[日志服务与SIEM（集成实战(二)：syslog篇](https://yq.aliyun.com/articles/684108)
- [本文Github样例](https://github.com/aliyun/aliyun-log-python-sdk/blob/master/tests/consumer_group_examples/copy_data_to_logstore.py)
