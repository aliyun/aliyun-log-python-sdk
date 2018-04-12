# 使用Log Handler自动上传Python日志

## 概述

使用Python SDK提供的Log Handler可以实现每一条Python程序的日志在不落盘的情况下自动上传到日志服务上。与写到文件再通过各种方式上传比起来，有如下优势：

1. 实时性：主动直接发送，不落盘
2. 吞吐量大，异步发送
3. 配置简单：无需修改程序，无需知道机器位置，修改程序配置文件即可生效


## 配置
Log Handler与Python logging模块完全兼容，参考[Python Logging](https://docs.python.org/2/library/logging.config.html)

Python logging模块允许通过编程或者文件的形式配置日志，如下我们通过文件配置`logging.conf`：

```yaml

[loggers]
keys=root,sls

[handlers]
keys=consoleHandler, slsHandler

[formatters]
keys=simpleFormatter, rawFormatter

[logger_root]
level=DEBUG
handlers=consoleHandler

[logger_sls]
level=INFO
handlers=consoleHandler, slsHandler
qualname=sls
propagate=0

[handler_consoleHandler]
class=StreamHandler
level=DEBUG
formatter=simpleFormatter
args=(sys.stdout,)

[handler_slsHandler]
class=aliyun.log.QueuedLogHandler
level=INFO
formatter=rawFormatter
args=(os.environ.get('ALIYUN_LOG_SAMPLE_ENDPOINT', ''), os.environ.get('ALIYUN_LOG_SAMPLE_ACCESSID', ''), os.environ.get('ALIYUN_LOG_SAMPLE_ACCESSKEY', ''), os.environ.get('ALIYUN_LOG_SAMPLE_TMP_PROJECT', ''), "logstore")

[formatter_simpleFormatter]
format=%(asctime)s - %(name)s - %(levelname)s - %(message)s


[formatter_rawFormatter]
format=%(message)s

```

这里我们配置了一个`root`和一个`sls`的Log Handler, 其中`sls`是实例化类`aliyun.log.QueuedLogHandler`，并传入参数([详细参数列表](https://aliyun-log-python-sdk.readthedocs.io/api.html#aliyun.log.QueuedLogHandler))如下：
```yaml
args=(os.environ.get('ALIYUN_LOG_SAMPLE_ENDPOINT', ''), os.environ.get('ALIYUN_LOG_SAMPLE_ACCESSID', ''), os.environ.get('ALIYUN_LOG_SAMPLE_ACCESSKEY', ''), os.environ.get('ALIYUN_LOG_SAMPLE_TMP_PROJECT', ''), "logstore")
```

注意：这里使用了`os.environ`来从环境变量中获取相关配置。这里也可以直接填写实际的值。

## 上传日志

使用logging配置文件并输出日志即可，日志会自动上传。

```python
import logging
import logging.config

# 配置
logging.config.fileConfig('logging.conf')
logger = logging.getLogger('sls')

# 使用logger
logger.info("test1")

try:
    1/0
except ZeroDivisionError as ex:
    logger.exception(ex)
```

之后日志即可自动上传到日志服务，如果要使用统计查询功能，最好打开索引。


## 配置日志服务logstore的索引

将接受日志的Logstore的索引打开，将特定域进行索引。推荐使用[CLI](http://aliyun-log-cli.readthedocs.io/)进行配置如下：


```shell
aliyunlog log update_index --project_name="project1" --logstore_name="logstore1" --index_detail="file:///Users/user1/loghandler_index.json"
```

参考：配置文件[python_logging_handler_index.json](https://github.com/aliyun/aliyun-log-cli/tree/master/tests/index/python_logging_handler_index.json)

## 调整收集日志域

目前支持如下的日志信息，默认会收集所有相关域：

| 域 | 说明 |
| -- | -- |
| message | 消息内容 | 
| record_name | logging handler的名字，上面例子是`sls` | 
| level | 级别，INFO、ERROR等 | 
| file_path | 代码文件全路径 |
| func_name | 所在函数名 |
| line_no | 行号 | 
| module | 所在模块 | 
| thread_id | 当前线程Id | 
| thread_name | 当前线程名 | 
| process_id | 当前进程Id | 
| process_name | 当前进程名 | 

参考类[QueuedLogHandler](https://aliyun-log-python-sdk.readthedocs.io/api.html#aliyun.log.QueuedLogHandler)的参数`fields`接受一个列表来调整想要配置的域。
进一步参考[日志域列表](https://aliyun-log-python-sdk.readthedocs.io/api.html#aliyun.log.LogFields)

下面例子中，我们修改之前的日志配置文件，只收集个别域如`module`、`func_name`等。（注意：`message`是一定会被收集的）：

```yaml
[handler_slsHandler]
class=aliyun.log.QueuedLogHandler
level=INFO
formatter=rawFormatter
args=('cn-beijing.log.aliyuncs.com', 'ak_id', 'ak_key', 'project1', "logstore1", 'mytopic', ['level', 'func_name', 'module', 'line_no']  )

```
