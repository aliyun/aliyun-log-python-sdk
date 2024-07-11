# filename: setup.py
import os
from setuptools import setup, Extension

if os.uname()[0] == 'Darwin':
    extra_compile_args=['-DLOG_KEY_VALUE_FLAG', '-stdlib=libc++', '-g']
else:
    extra_compile_args=['-DLOG_KEY_VALUE_FLAG','-std=c++11', '-g']

slspb = Extension('slspb', sources=["slspb/slspb.cpp","slspb/log_builder.c", "slspb/sds.c","slspb/lz4.c"],extra_compile_args=extra_compile_args)
setup(
   name="slspb",
   version="1.0",
   description='A cpp implement for sls protobuf parse',
   ext_modules=[slspb],
   test_suite = 'tests.test_suite'
)
