# 日志服务与SIEM（如IBM QRadar）集成方案：syslog方式

# 背景信息
## 背景
在[日志服务与SIEM（如Splunk）集成方案（一）](https://yq.aliyun.com/articles/684035)中，我们介绍了如何使用消费组的技术，实现稳定、高性能与可扩展的数据传输，并使用了**SIEM的接口（例如Splunk的HEC）**来对接。
在现实中，**syslog**也是一个常见的日志通道，大部分物理设备、交换机路由器以及服务器等，都支持通过**syslog**来发送日志，因此几乎所有的SIEM(如`IBM Qradar`, `HP Arcsight`等)也支持`syslog`渠道接受日志。
本文将重点介绍如何使用`syslog`与SIEM(如`IBM Qradar`, `HP Arcsight`等)对接，确保传输的性能与可靠性，以便确保阿里云上的所有法规、审计、与其他相关日志能够导入到您的安全运维中心（SOC）中。

## 概念
**syslog**主要是标准协议[RFC5424](https://datatracker.ietf.org/doc/rfc5424)和[RFC3164](https://tools.ietf.org/html/rfc3164)定义了相关格式规范，协议`RFC5424`(2009年发布）是升级版本，并废弃了`RFC3164`(2001年发布）。因为新版兼容旧版，且新版本解决了很多问题，因此推荐使用`RFC5424`协议。

**syslog over TCP/TLS：**syslog只是规定了日志格式，理论上TCP和UDP都可以支持syslog，可以较好的确保数据传输稳定性。协议[RFC5425](https://tools.ietf.org/html/rfc5425)也定义了TLS的安全传输层，如果您的SIEM支持TCP通道，甚至TLS，那么建议优先使用。


**syslog facility：**早期Unix定义的程序组件，一般如下定义，这里我们可以选择`user`作为默认组件。
| Facility | 代码 | 关键字 | 描述 |
| --- | --- | --- | --- |
| 0 | kern | Kernel | messages |
| 1 | user | User-level | messages |
| 2 | mail | Mail | system |
| 3 | daemon | System | daemons |
| 4 | auth | Security/authentication | messages |
| 5 | syslog | Messages | generated | internally | by | syslogd |
| 6 | lpr | Line | printer | subsystem |
| 7 | news | Network | news | subsystem |
| 8 | uucp | UUCP | subsystem |
| 9 | cron | Clock | daemon |
| 10 | authpriv | Security/authentication | messages |
| 11 | ftp | FTP | daemon |
| 12 | ntp | NTP | subsystem |
| 13 | security | Log | audit |
| 14 | console | Log | alert |
| 15 | solaris-cron | Scheduling | daemon |
| 16–23 | local0 | – | local7 | Locally | used | facilities |

**syslog severity：**定义了日志的级别，可以根据需要设置特定内容的日志为较高的级别。默认一般用`info`。
|值|严重度|关键字|描述 |
| --- | --- | --- | --- |
|0|Emergency|emerg|System is unusable|
|1|Alert|alert|A condition that should be corrected immediately, such as a corrupted system database.|
|2|Critical|crit|Critical conditions|
|3|Error|err|error|Error conditions|
|4|Warning|warning|warn|Warning conditions|
|5|Notice|notice|Conditions that are not error conditions, but that may require special handling.|
|6|Informational|info|Informational messages|
|7|Debug|debug|Messages that contain information normally of use only when debugging a program.|


# 集成方案建议

## 假设
这里假设您的SIEM（如IBM QRadar）位于组织内部环境（on-premise）中，而不是云端。为了安全考虑，没有任何端口开放让外界环境来访问此SIEM。


## 概览
推荐使用SLS消费组构建程序来从SLS进行实时消费，然后通过`syslog over TCP/TLS`来发送日志给SIEM。
关于消费组的相关概念、以及程序部署相关的环境需求等。可以直接参考[日志服务与SIEM（如Splunk）集成方案（一）](https://yq.aliyun.com/articles/684035)。
本文着重介绍与SIEM通讯的`syslog`部分。如果您的SIEM支持TCP通道，甚至TLS，那么建议优先使用。

![image](https://yqfile.alicdn.com/82c4271c4a0d6445d041dd59bc469cdb0cbecbdc.png)


# 使用消费组编程


### 程序配置
如下展示如何配置程序：
1. 配置程序日志文件，以便后续测试或者诊断可能的问题。
2. 基本的日志服务连接与消费组的配置选项。
3. 消费组的一些高级选项（性能调参，不推荐修改）。
4. SIEM的syslog server相关参数与选项。

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

    # syslog options
    settings = {
                "host": "1.2.3.4", # 必选
                "port": 514,       # 必选, 端口
                "protocol": "tcp", # 必选, tcp, udp或 tls (仅Python3)
                "sep": "||",       # 必选, key=value键值对的分隔符，这里用||分隔
                "cert_path": None, # 可选, TLS的证书位置
                "timeout": 120,    # 可选, 超时时间，默认120秒
                "facility": syslogclient.FAC_USER,  # 可选, 可以参考其他syslogclient.FAC_*的值
                "severity": syslogclient.SEV_INFO,  # 可选, 可以参考其他syslogclient.SEV_*的值
                "hostname": None,  # 可选，机器名，默认选择本机机器名
                "tag": None        # 可选，标签，默认是 -
            }

    return option, settings
```


### 数据消费与转发
如下代码展示如何从SLS拿到数据后转发给SIEM syslog服务器。主要这里依赖了一个库`syslogclient.py`，也在我们的样例代码库中可以找到，是一个标准syslog协议的实现版本。

请仔细阅读代码中相关注释并根据需要调整格式化方式：

```python
from syslogclient import SyslogClientRFC5424 as SyslogClient

class SyncData(ConsumerProcessorBase):
    """
    这个消费者从SLS消费数据并发送给syslog server
    """
    def __init__(self, splunk_setting):
	    """初始化并验证syslog server连通性"""
        super(SyncData, self).__init__()   # remember to call base's init

        assert target_setting, ValueError("You need to configure settings of remote target")
        assert isinstance(target_setting, dict), ValueError("The settings should be dict to include necessary address and confidentials.")

        self.option = target_setting
        self.protocol = self.option['protocol']
        self.timeout = int(self.option.get('timeout', 120))
        self.sep = self.option.get('sep', "||")
        self.host = self.option["host"]
        self.port = int(self.option.get('port', 514))
        self.cert_path=self.option.get('cert_path', None)

        # try connection
        with SyslogClient(self.host, self.port, proto=self.protocol, timeout=self.timeout, cert_path=self.cert_path) as client:
            pass

    def process(self, log_groups, check_point_tracker):
        logs = PullLogResponse.loggroups_to_flattern_list(log_groups, time_as_str=True, decode_bytes=True)
        logger.info("Get data from shard {0}, log count: {1}".format(self.shard_id, len(logs)))
        try:
            with SyslogClient(self.host, self.port, proto=self.protocol, timeout=self.timeout, cert_path=self.cert_path) as client:
                for log in logs:
                    # Put your sync code here to send to remote.
                    # the format of log is just a dict with example as below (Note, all strings are unicode):
                    #    Python2: {"__time__": "12312312", "__topic__": "topic", u"field1": u"value1", u"field2": u"value2"}
                    #    Python3: {"__time__": "12312312", "__topic__": "topic", "field1": "value1", "field2": "value2"}
                    # suppose we only care about audit log
                    timestamp = datetime.fromtimestamp(int(log[u'__time__']))
                    del log['__time__']

                    io = six.StringIO()
                    first = True
					# TODO： 这里可以根据需要修改格式化内容，这里使用Key=Value传输，并使用默认的||进行分割
                    for k, v in six.iteritems(log):
                        io.write("{0}{1}={2}".format(self.sep, k, v))

                    data = io.getvalue()

					# TODO：这里可以根据需要修改facility或者severity
                    client.log(data, facility=self.option.get("facility", None), severity=self.option.get("severity", None), timestamp=timestamp, program=self.option.get("tag", None), hostname=self.option.get("hostname", None))

        except Exception as err:
            logger.debug("Failed to connect to remote syslog server ({0}). Exception: {1}".format(self.option, err))

            # TODO: 需要添加一些错误处理的代码，例如重试或者通知等
            raise err

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
假设程序命名为"sync_data.py"，需要把可以如下启动：

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

**限制与约束**
每一个日志库（logstore）最多可以配置10个消费组，如果遇到错误`ConsumerGroupQuotaExceed`则表示遇到限制，建议在控制台端删除一些不用的消费组。

## 部署、管理、监控、性能、安全性相关
请参考[日志服务与SIEM（如Splunk）集成方案（一）](https://yq.aliyun.com/articles/684035)中相关内容。

## 其他
### syslog over TLS
如果SIEM支持基于TCP甚至TLS的syslog通道，那么可以在配置的时候配置额外的`proto`为`TLS`，并且配置正确的SSL的证书即可。



## 更多案例
- 日志服务Python消费组实战（一）：[日志服务与SIEM（如Splunk）集成实战](https://yq.aliyun.com/articles/684035)
- 日志服务Python消费组实战（二）：[实时日志分发](https://yq.aliyun.com/articles/684081)
- 日志服务Python消费组实战（三）：[实时跨域监测多日志库数据](https://yq.aliyun.com/articles/684108)
- 日志服务Python消费组实战（三）：[日志服务与SIEM（集成实战(二)：syslog篇](https://yq.aliyun.com/articles/684108)
- 本文Github样例：[sync_data_to_syslog.py](https://github.com/aliyun/aliyun-log-python-sdk/blob/master/tests/consumer_group_examples/sync_data_to_syslog.py)、[syslogclient.py](https://github.com/aliyun/aliyun-log-python-sdk/blob/master/tests/consumer_group_examples/syslogclient.py)