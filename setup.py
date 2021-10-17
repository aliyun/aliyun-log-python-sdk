#!/usr/bin/env python
# encoding: utf-8
#
# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

"""Setup script for log service SDK.

You need to install google protocol buffer, setuptools and python-requests.
https://code.google.com/p/protobuf/
https://pypi.python.org/pypi/setuptools
http://docs.python-requests.org/

Depending on your version of Python, these libraries may also should be installed:
http://pypi.python.org/pypi/simplejson/

"""

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


import sys
import re

install_requires_common = ['requests', 'six', 'python-dateutil', 'elasticsearch', 'jmespath']
install_requires = []

if sys.version_info[:2] == (2, 6):
    install_requires = ['protobuf <= 3.4.0', 'enum34', 'futures']
elif sys.version_info[0] == 2:
    install_requires = ['protobuf <= 3.17.3', 'enum34', 'futures', 'dateparser <= 0.7.6']
elif sys.version_info[:2] == (3, 3):
    install_requires = ['protobuf>=3.4.0', 'enum34',  'dateparser']
elif sys.version_info[0] == 3:
    install_requires = ['protobuf>=3.4.0',  'dateparser']

install_requires.extend(install_requires_common)

packages = [
            'aliyun',
            'aliyun.log',
            'aliyun.log.ext',
            'aliyun.log.etl_core',
            'aliyun.log.etl_core.transform',
            'aliyun.log.etl_core.trans_comp',
            'aliyun.log.consumer',
            'aliyun.log.es_migration'
            ]

version = ''
with open('aliyun/log/version.py', 'r') as fd:
    version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
                        fd.read(), re.MULTILINE).group(1)

classifiers = [
            'Development Status :: 5 - Production/Stable',
            'License :: OSI Approved :: MIT License',
            'Operating System :: OS Independent',
            'Programming Language :: Python :: 2.6',
            'Programming Language :: Python :: 2.7',
            'Programming Language :: Python :: 3.3',
            'Programming Language :: Python :: 3.4',
            'Programming Language :: Python :: 3.5',
            'Programming Language :: Python :: 3.6',
            'Programming Language :: Python :: 3.7',
            'Programming Language :: Python :: Implementation :: PyPy'
    ]


long_description = """
Python SDK for Alicloud Log Service 
http://aliyun-log-python-sdk.readthedocs.io
"""

setup(
      name='aliyun-log-python-sdk',
      version=version,
      description='Aliyun log service Python client SDK',
      author='Aliyun',
      url='https://github.com/aliyun/aliyun-log-python-sdk',
      install_requires=install_requires,
      packages=packages,
      classifiers=classifiers,
      long_description=long_description
     )
