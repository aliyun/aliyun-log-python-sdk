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


install_requires = ['requests', 'protobuf', 'six', 'enum34', 'futures']

packages = [
            'aliyun',
            'aliyun.log',
            'aliyun.log.consumer',
            'aliyun.log.consumer.loghub_exceptions'
            ]

version = '0.6.5'

classifiers = [
            'Development Status :: 5 - Production/Stable',
            'Programming Language :: Python :: 2.6',
            'Programming Language :: Python :: 2.7',
            'Programming Language :: Python :: 3.3',
            'Programming Language :: Python :: 3.4',
            'Programming Language :: Python :: 3.5',
            'Programming Language :: Python :: 3.6'
            ]

try:
    import pypandoc
    long_description = pypandoc.convert('README.md', 'rst')
except(IOError, ImportError):
    print("........")
    long_description = open('README.md').read()

setup(
      name='aliyun-log-python-sdk',
      version=version,
      description='Aliyun log service Python client SDK',
      author='Aliyun',
      url='http://www.aliyun.com/product/sls',
      install_requires=install_requires,
      packages=packages,
      classifiers=classifiers,
      long_description=long_description
     )

