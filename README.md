# User Guide

[![Documentation Status](https://readthedocs.org/projects/aliyun-log-python-sdk/badge/?version=latest)](http://aliyun-log-python-sdk.readthedocs.io/?badge=latest)
[![Pypi Version](https://badge.fury.io/py/aliyun-log-python-sdk.svg)](https://badge.fury.io/py/aliyun-log-python-sdk)
[![Travis CI](https://travis-ci.org/aliyun/aliyun-log-python-sdk.svg?branch=master)](https://travis-ci.org/aliyun/aliyun-log-python-sdk)
[![Development Status](https://img.shields.io/pypi/status/aliyun-log-python-sdk.svg)](https://pypi.python.org/pypi/aliyun-log-python-sdk/)
[![Python version](https://img.shields.io/pypi/pyversions/aliyun-log-python-sdk.svg)](https://pypi.python.org/pypi/aliyun-log-python-sdk/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/aliyun/aliyun-log-python-sdk/blob/master/LICENSE)

[中文版README](https://github.com/aliyun/aliyun-log-python-sdk/blob/master/README_CN.md)

## Introduction

This is the open source version of Python SDK for AliCloud Log Service. It's a Python programming interfaces of Alicloud
Log Service, providing encapsulations of Log Service Rest API. It helps Pythoner to connect to Alicloud Log Service more
efficiently.

Refer to the doc: [http://aliyun-log-python-sdk.readthedocs.io/](http://aliyun-log-python-sdk.readthedocs.io/).

Don't want to write code? Try the [CLI](http://aliyun-log-cli.readthedocs.io/en/latest/) which covers almost all features of this SDK.

### Features
1. Wrap all SLS Rest API (Management, data manipulation, consumer group etc)
2. Consumer Group high level Class Support.
3. Python Logging handler util
4. High level operations: auto paging, auto retry till complete, copy project/logstore settings, arrange_shard, resource usage etc.
5. Elasticsearch data migration
6. ETL feature: copy data cross logstore, transform data cross logstore with powerful ETL config.


### Supported Python：

1. Python 2 with version >= 2.7  
2. Python 3 with version >= 3.7  

### Supported Log Service API
1. Log Service API 0.6

### Change Logs

[Change Logs](https://github.com/aliyun/aliyun-log-python-sdk/releases)


## Installation
```shell
pip install -U aliyun-log-python-sdk
```

## Sample Code:
- [Sample Code](https://github.com/aliyun/aliyun-log-python-sdk/tree/master/tests)


## Complete API Reference
- [API Reference](http://aliyun-log-python-sdk.readthedocs.io/api.html)

## Other resources

1. Alicloud Log Service homepage：https://www.alibabacloud.com/product/log-service
2. Alicloud Log Service doc：https://www.alibabacloud.com/help/product/28958.htm
3. for any issues, please submit support tickets
