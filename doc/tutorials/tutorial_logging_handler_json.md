# 使用Python Log Handler自动上传并解析JSON格式的日志

## 概述

使用Python SDK提供的Log Handler可以实现每一条Python程序的日志在不落盘的情况下自动上传到日志服务上。与写到文件再通过各种方式上传比起来，有如下优势：

1. 实时性：主动直接发送，不落盘
2. 吞吐量大，异步发送
3. 配置简单：无需修改程序，无需知道机器位置，修改程序配置文件即可生效
4. 智能解析: 自动解析日志中JSON和KV格式信息

本篇主要如何打开自动解析`JSON格式`的功能, 关于如何配置并使用的基本信息, 请参考[使用Log Handler自动上传Python日志](https://aliyun-log-python-sdk.readthedocs.io/tutorials/tutorial_logging_handler.html)
 
## 解决的问题

在程序中, 有时我们需要将特定数据输出到日志中以便跟踪, 例如:

```python
data = {'name':u"小明", 'score': 100.0}
```

一般情况下, 我们可以直接输出数据, 如下:

```python
response_data = {'name':u'小明', 'score': 100.0}
logger.info(response_data)
```

这样会输出的消息为:
```shell
{'name':u'小明', 'score': 100.0}
```

因为Python格式化的原因, 数据的字符串形式并不是真正的JSON格式. 并且我们期望在上传到日志服务时可以自动解析出域`name`和`score`字段. 使用Python Handler的简单配置即可做到. 如下.

## 通过Logging的配置文件

参考[Logging Handler的详细配置](https://aliyun-log-python-sdk.readthedocs.io/tutorials/tutorial_logging_handler.html#id2), 将其中参数列表修改为:

args=(os.environ.get('ALIYUN_LOG_SAMPLE_ENDPOINT', ''), os.environ.get('ALIYUN_LOG_SAMPLE_ACCESSID', ''), os.environ.get('ALIYUN_LOG_SAMPLE_ACCESSKEY', ''), os.environ.get('ALIYUN_LOG_SAMPLE_TMP_PROJECT', ''), "logstore", None, None, None, None, True)

最后一个参数对应了Logging Handler的[详细参数](https://aliyun-log-python-sdk.readthedocs.io/api.html#aliyun.log.QueuedLogHandler)的`extract_json`参数.

注意, 受限于[Python Logging](https://docs.python.org/2/library/logging.config.html)的限制, 这里只能用无名参数, 依次传入. 对于不改的参数, 用`None`占位.

## 通过代码以JSON形式配置
如果期望更加灵活的配置, 也可以使用代码配置, 如下将参数`extract_json`设置为`True`即可.

```python
#encoding: utf8
import logging, logging.config, os

# 配置
conf = {'version': 1,
        'formatters': {'rawformatter': {'class': 'logging.Formatter',
                                        'format': '%(message)s'}
                       },
        'handlers': {'sls_handler': {'()':
                                     'aliyun.log.QueuedLogHandler',
                                     'level': 'INFO',
                                     'formatter': 'rawformatter',

                                     # custom args:
                                     'end_point': os.environ.get('ALIYUN_LOG_SAMPLE_ENDPOINT', ''),
                                     'access_key_id': os.environ.get('ALIYUN_LOG_SAMPLE_ACCESSID', ''),
                                     'access_key': os.environ.get('ALIYUN_LOG_SAMPLE_ACCESSKEY', ''),
                                     'project': 'project1',
                                     'log_store': "logstore1",
                                     'extract_json': True
                                     }
                     },
        'loggers': {'sls': {'handlers': ['sls_handler', ],
                                   'level': 'INFO',
                                   'propagate': False}
                    }
        }
logging.config.dictConfig(conf)

# 使用
logger = logging.getLogger('sls')


response_data = {'name':u'小明', 'score': 100.0}
logger.info(response_data)

```


## 域名冲突

当关键字和[内置日志域](https://aliyun-log-python-sdk.readthedocs.io/tutorials/tutorial_logging_handler.html#id4)冲突时, 需要做一些调整, 例如:

```python
c1 = 'student="xiao ming" level=3'
```

这里的`level`和日志域的内建表示日志级别冲突了, 可以通过参数`buildin_fields_prefix` / `buildin_fields_suffix`给系统日志域添加前缀后缀;
或者通过参数`extract_kv_prefix`和`extract_kv_suffix`给抽取的域添加前缀后缀来解决.


## 其他定制参数

自动抽取KV也支持更多其他相关参数如下:

参数                      |            作用       | 默认值
-----------------        |   ------------------       |------
extract_json             | 是否自动解析json    | False
extract_json_drop_message| 当打开自动解析json并且解析到时, 是否丢弃掉默认的`message`域, 某些情况下为了节省空间可以打开. | False
extract_json_prefix      | 给解析的域添加前缀, 某些情况下为了避免冲突使用  | 空串
extract_json_suffix      | 给解析的域添加后缀, 某些情况下为了避免冲突使用  | 空串
buildin_fields_prefix    | 给系统域添加前缀, 某些情况下为了避免冲突使用 | 空串
buildin_fields_suffix    | 给系统域添加后缀, 某些情况下为了避免冲突使用 | 空串

