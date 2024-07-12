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
from setuptools import setup, Extension
from setuptools.command.build_ext import build_ext
import re


requirements_py3 = [
    'six',
    'requests',
    'python-dateutil',
    'elasticsearch',
    'jmespath',
    'dateparser',
    'deprecated',
    'protobuf>3.4.0,<4.0.0',
]
requirements_py2 = [
    'six==1.14.0',
    'requests==2.23.0',
    'python-dateutil==2.8.1',
    'elasticsearch==7.17.2',
    'jmespath==0.9.5',
    'enum34==1.1.10',
    'futures==3.3.0',
    'protobuf>3.4.0,<4.0.0',
    'regex==2021.3.17',
    'tzlocal==2.0.0',
    'deprecated',
    'lz4a==0.7.0',
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

test_requirements = [
    'lz4'
]

packages = [
    'aliyun',
    'aliyun.log',
    'aliyun.log.ext',
    'aliyun.log.etl_core',
    'aliyun.log.etl_core.transform',
    'aliyun.log.etl_core.trans_comp',
    'aliyun.log.consumer',
    'aliyun.log.es_migration',
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
    'Programming Language :: Python :: Implementation :: PyPy',
]


long_description = """
Python SDK for Alicloud Log Service
http://aliyun-log-python-sdk.readthedocs.io
"""

class BuildExt(build_ext):
    def build_extensions(self):
        # windows msvc
        if sys.platform == 'win32' and self.compiler.compiler_type == 'msvc':
            for ext in self.extensions:
                ext.extra_compile_args = ['/EHsc', '/std:c++11', '/DLOG_KEY_VALUE_FLAG']
        # macos
        elif sys.platform == 'darwin':
            for ext in self.extensions:
                ext.extra_compile_args = ['-DLOG_KEY_VALUE_FLAG', '-stdlib=libc++']
        # linux and other
        else:
            for ext in self.extensions:
                ext.extra_compile_args = ['-DLOG_KEY_VALUE_FLAG', '-std=c++11']

        build_ext.build_extensions(self)

extension_source_dir = 'aliyun/log/pb'
extension_sources = ['pb.cpp',
                     'log_builder.c',
                     'sds.c',
                     'lz4.c']
slspb = Extension('aliyun_log_pb',
                  sources=[extension_source_dir + '/' + s for s in extension_sources],
                  language='c++')


setup(
    name='aliyun-log-python-sdk',
    version=version,
    description='Aliyun log service Python client SDK',
    author='Aliyun',
    url='https://github.com/aliyun/aliyun-log-python-sdk',
    install_requires=requirements,
    extras_require = {
        'test': test_requirements,
    },
    ext_modules=[slspb],
    packages=packages,
    classifiers=classifiers,
    long_description=long_description,
    cmdclass={
        'build_ext': BuildExt,
    },
)
