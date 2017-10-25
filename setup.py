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

import sys
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

if float("%d.%d" % sys.version_info[:2]) < 2.6 or float("%d.%d" % sys.version_info[:2]) >= 3.0:
    sys.stderr.write("Your Python version %d.%d.%d is not supported.\n" % sys.version_info[:3])
    sys.stderr.write("log service SDK requires Python version 2.6 and 2.7.\n")
    sys.exit(1)

install_requires = []
try:
    import json
except ImportError:
    try:
        import simplejson
    except ImportError:
        install_requires.append('simplejson')

packages = [
            'aliyun',
            'aliyun.log'
            ]

version = '0.6.4'

classifiers = [
            'Development Status :: 5 - Production/Stable',
            'Programming Language :: Python :: 2.6',
            'Programming Language :: Python :: 2.7'
            ]

setup(
      name='aliyun-log-python-sdk',
      version=version,
      description='Aliyun log service Python client SDK',
      author='Aliyun',
      url='http://www.aliyun.com/product/sls',
      install_requires=install_requires,
      packages=packages,
      classifiers=classifiers
     )

