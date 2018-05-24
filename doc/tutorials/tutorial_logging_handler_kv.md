# 使用Python Log Handler自动上传并解析KV格式的日志

## 概述

使用Python SDK提供的Log Handler可以实现每一条Python程序的日志在不落盘的情况下自动上传到日志服务上。与写到文件再通过各种方式上传比起来，有如下优势：

1. 实时性：主动直接发送，不落盘
2. 吞吐量大，异步发送
3. 配置简单：无需修改程序，无需知道机器位置，修改程序配置文件即可生效
4. 智能解析: 自动解析日志中JSON和KV格式信息

本篇主要如何打开自动解析`KV格式`的功能, 关于如何配置并使用的基本信息, 请参考[使用Log Handler自动上传Python日志](https://aliyun-log-python-sdk.readthedocs.io/tutorials/tutorial_logging_handler.html)
 
## 解决的问题

在程序中, 有时我们需要将特定数据输出到日志中以便跟踪, 例如:

```python
data = {'name':'xiao ming', 'score': 100.0}
```

一般情况下, 我们会格式化数据内容, 附加其他信息并输出:

```python
data = {'name':'xiao ming', 'score': 100.0}
logger.error('get some error when parsing data. name="{}" score={}'.format(data['name'], data['score']))
```

这样会输出的消息为:
```shell
get some error when parsing data. name="xiao ming" score=100.0
```

我们期望在上传到日志服务时可以自动解析出域`name`和`score`字段. 使用Python Handler的简单配置即可做到. 如下.

## 通过Logging的配置文件

参考[Logging Handler的详细配置](https://aliyun-log-python-sdk.readthedocs.io/tutorials/tutorial_logging_handler.html#id2), 将其中参数列表修改为:

args=(os.environ.get('ALIYUN_LOG_SAMPLE_ENDPOINT', ''), os.environ.get('ALIYUN_LOG_SAMPLE_ACCESSID', ''), os.environ.get('ALIYUN_LOG_SAMPLE_ACCESSKEY', ''), os.environ.get('ALIYUN_LOG_SAMPLE_TMP_PROJECT', ''), "logstore", None, None, None, None, None, None, None, None, True)

最后一个参数对应了Logging Handler的[详细参数](https://aliyun-log-python-sdk.readthedocs.io/api.html#aliyun.log.QueuedLogHandler)的`extract_kv`参数.

注意, 受限于[Python Logging](https://docs.python.org/2/library/logging.config.html)的限制, 这里只能用无名参数, 依次传入. 对于不改的参数, 用`None`占位.

## 通过代码以JSON形式配置
如果期望更加灵活的配置, 也可以使用代码配置, 如下将参数`extract_kv`设置为`True`即可.

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
                                     'extract_kv': True
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
logger.error("get error, reason=103 return_code=333 agent_type=ios")

```

## 支持KV的格式

默认支持key=value的格式, 也就是等号`=`分隔的值. 其中关键字`key`的范围是: 中日文, 字母数字, 下划线, 点和横线. 值`value`在有双引号括起来的情况下是除了双引号的任意字符. 在没有双引号括起来的情况下和关键字是一样的. 如下都是支持的:

```python
c1 = "i=c1, k1=v1,k2=v2 k3=v3"
c2 = 'i=c2, k1=" v 1 ", k2="v 2" k3="~!@#=`;.>"'  # 双引号
c3 = 'i=c3, k1=你好 k2=他们'       # utf8
c4 = u'i=c4, 姓名=小明 年龄=中文 '   # utf8
c5 = u'i=c5, 姓名="小明" 年龄="中文"'# utf8
c6 = u'i=c6, 姓名=中文 年龄=中文'    # unicode
c7 = u'i=c7, 姓名="小明" 年龄=中文 ' # unicode
c8 = """i=c8, k1="hello           # 换行
world" k2="good
morning"
"""
```

## 自定义分隔符

默认通过等号`=`分隔, 也可以通过参数`extract_kv_sep`修改, 例如冒号:

```shell
c9 = 'k1:v1 k2:v2'
```

有时我们的分隔符是混合的, 有时为`=`有时为`:`, 如下: 

```python
c10 = 'k1=v1 k2:v2'
c11 = "k3 = v3"
c12 = "k4 : v4"
```

可以传入一个正则表达式给参数`extract_kv_sep`即可, 例如上面的情况可以传入`(?:=|:)`, 这里使用可非捕获分组`(?:)`, 再用`|`将各种可能的分隔符写入即可. 


## 域名冲突

当关键字和[内置日志域](https://aliyun-log-python-sdk.readthedocs.io/tutorials/tutorial_logging_handler.html#id4)冲突时, 需要做一些调整, 例如:

```python
c1 = 'student="xiao ming" level=3'
```

这里的`level`和日志域的内建表示日志级别冲突了, 可以通过参数`buildin_fields_prefix` / `buildin_fields_suffix`给系统日志域添加前缀后缀;
或者通过参数`extract_kv_prefix`和`extract_kv_suffix`给抽取的域添加前缀后缀来解决.


## 其他定制参数

自动抽取KV也支持更多其他相关参数如下:

| 参数 | 作用 | 默认值 |
| -- | -- | -- |
| extract_kv | 是否自动解析KV | False |
| extract_kv_drop_message | 匹配KV后是否丢弃掉默认的message域 | False |
| extract_kv_prefix | 给解析的域添加前缀  | 空串 |
| extract_kv_suffix | 给解析的域添加后缀  | 空串 |
| extract_kv_sep | 关键字和值的分隔符  | = |
| buildin_fields_prefix | 给系统域添加前缀 | 空串 |
| buildin_fields_suffix | 给系统域添加后缀 | 空串 |


