import os
from aliyun.log.etl_core.restrict_config_parser import RestrictConfigParser, InvalidETLConfig

base_dir = os.path.dirname(__file__)


def parse_config(config_path):
        md_path = os.path.sep.join([base_dir, config_path])
        if os.path.isfile(md_path):
            code = open(md_path, 'r').read()
        else:
            code = config_path
        RestrictConfigParser().parse(code)


def check_module(md_path, error=False):
    if error:
        try:
            parse_config(md_path)
            raise Exception("[Not Meet] expecting exception, but no error")
        except InvalidETLConfig as ex:
            print("[Meet] got exception: ", ex)
    else:
        parse_config(md_path)


check_module('./restrict_config_test_data//config1_safe.py')
check_module('./restrict_config_test_data//config1_fail.py', True)

check_module('./restrict_config_test_data//config2_fail1.py', True)
check_module('./restrict_config_test_data//config2_fail2.py', True)
check_module('./restrict_config_test_data//config2_fail3.py', True)

check_module('./restrict_config_test_data//config3_safe.py')
check_module('./restrict_config_test_data//config3_fail.py', True)

check_module('./restrict_config_test_data//config4_safe.py')
check_module('./restrict_config_test_data//config5_safe.py')


check_module('./restrict_config_test_data//config6_safe.py')
check_module('./restrict_config_test_data//config7_safe.py')


code = """
import re
"""
check_module(code, True)

code = """
from aliyun.log.etl_core import V
"""
check_module(code, True)


code = """
from aliyun.log.etl_core import *
"""
check_module(code)


code = """
KEEP_FIELDS_f1 = {"abc": V("xyz")}
"""
check_module(code)

code = """
KEEP_FIELDS_f1 = {"abc".lower(): "xyz"}
"""
check_module(code, True)


code = """
KEEP_FIELDS_f1 = {"abc": lambda e: 'v' in e }
"""
check_module(code, True)

code = """
KEEP_FIELDS_f1 = {"abc": Y("x") }
"""
check_module(code, True)


c = """
DROP_FIELDS_v1 = {'k1': str.isdigit}
"""
check_module(c, True)


c = """
DROP_FIELDS_v2 = {'k3': lambda x: x.isupper()}
"""
check_module(c, True)


c = """
TRANSFORM_EVENT_v16 = lambda x: {'k3': 'abc123'}
"""
check_module(c, True)

c = """
KEEP_FIELDS_f1 = {"abc": "abc" + "xyz"}
"""
check_module(c, True)

c = """
KEEP_FIELDS_f1 = {"abc": f"abc{xyz}"}
"""
try:
    check_module(c, True)
except SyntaxError:
    pass


c = """
KEEP_FIELDS_f1 = {"abc": (1,2,lambda x:x)}
"""
check_module(c, True)


c = """
aliyun.log.etl_core.restrict_config_parser.RestrictConfigParser = None
"""
check_module(c, True)

c = """
KEEP_FIELDS_f1 = __import__("os")
"""
check_module(c, True)


c = """
KEEP_FIELDS_f1 = "abc".__class__
"""
check_module(c, True)

