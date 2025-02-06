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


requirements_py3 = [
    'six',
    'requests',
    'python-dateutil',
    'elasticsearch',
    'jmespath',
    'dateparser',
    'protobuf>=3.20.3,<6.0.0',
    'lz4',
]
requirements_py2 = [
    'six==1.14.0',
    'requests==2.23.0',
    'python-dateutil==2.8.1',
    'elasticsearch==7.17.2',
    'jmespath==0.9.5',
    'enum34==1.1.10',
    'futures==3.3.0',
    'protobuf>3.4.0,<=3.17.3', # 3.18.0 is the last version support py2, but yanked
    'regex==2021.3.17',
    'tzlocal==2.0.0',
    'lz4==2.2.1',
]

test_requirements = [
    'pytest',
    'lz4',
    'virtualenv',
    'zstandard'
]

requirements = []
if sys.version_info[:2] == (2, 6):
    requirements = requirements_py2
elif sys.version_info[0] == 2:
    requirements = requirements_py2 + ['dateparser<=0.7.6']
elif sys.version_info[:2] == (3, 3):
    requirements = requirements_py3 + ['enum34']
elif sys.version_info[0] == 3:
    requirements = requirements_py3


packages = [
    'aliyun',
    'aliyun.log',
    'aliyun.log.ext',
    'aliyun.log.etl_core',
    'aliyun.log.etl_core.transform',
    'aliyun.log.etl_core.trans_comp',
    'aliyun.log.consumer',
    'aliyun.log.es_migration',
    'aliyun.log._proto_py2',
]

version = ''
with open('aliyun/log/version.py', 'r') as fd:
    version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
                        fd.read(), re.MULTILINE).group(1)

classifiers = [
    'Development Status :: 5 - Production/Stable',
    'License :: OSI Approved :: MIT License',
    'Operating System :: OS Independent',
    'Programming Language :: Python :: 2.7',
    'Programming Language :: Python :: 3.7',
    'Programming Language :: Python :: 3.8',
    'Programming Language :: Python :: 3.9',
    'Programming Language :: Python :: 3.10',
    'Programming Language :: Python :: 3.11',
    'Programming Language :: Python :: 3.12',
    'Programming Language :: Python :: Implementation :: PyPy',
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
    install_requires=requirements,
    packages=packages,
    classifiers=classifiers,
    long_description=long_description,
    extras_require = {
        'test': test_requirements,
    },
)
