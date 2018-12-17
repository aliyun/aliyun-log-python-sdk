#encoding: utf8

from aliyun.log.etl_core import *
from aliyun.log.etl_core.config_parser import ConfigParser
from aliyun.log.etl_core.runner import Runner
import json
import os
from time import time
from random import randint
import six

t = transform


def test_condition():
    event = {'k1': '123', 'k2': 'abc', 'k3': "abc123"}

    # simple bool
    assert condition(True)(event)
    assert not condition(False)(event)
    assert condition([True])(event)
    assert not condition([False])(event)

    # dict - string
    assert condition({'k1': r'\d+'})(event)
    assert condition([{'k1': r'\d+'}])(event)
    assert condition({'k2': r'\w+'})(event)
    assert not condition({'k3': r'\d+'})(event)

    # not exist
    assert condition({'k4': None})(event)
    assert not condition({'k4': r'.+'})(event)

    # dict - or
    assert condition([{'k1': r'\d+'}, {'k2': r'\d+'}])(event)
    assert condition([{'k1': r'\d+'}, {'k4': '.+'}])(event)
    assert not condition([{'k1': r'[a-z]+'}, {'k4': r'\w+'}])(event)

    # dict - and
    assert condition([{'k1': r'\d+', 'k2': r'\w+'}])(event)
    assert not condition([{'k1': r'\d+', 'k4': '.+'}])(event)
    assert not condition([{'k1': r'\d+', 'k3': r'\d+'}])(event)

    # dict - lambda
    assert condition({'k1': str.isdigit})(event)
    assert condition({'k2': str.islower})(event)
    assert not condition({'k3': lambda x: x.isupper()})(event)

    # dict - bool
    assert condition({'k1': True})(event)
    assert not condition({'k4': True})(event)

    # lambda
    assert condition(lambda e: 'k1' in e and e['k1'].isdigit())(event)
    assert not condition(lambda e: 'k5' in e)(event)

def test_regex():
    """

    """

    # dict - append
    assert t({'k3': 'abc123'})({'k1': '123', 'k2': 'abc'}) == {'k1': '123', 'k2': 'abc', 'k3': "abc123"}

    # dict - overwrite
    assert t({'k1': 'abc123'})({'k1': '123', 'k2': 'abc'}) == {'k1': 'abc123', 'k2': 'abc'}

    # lambda overwrite all
    assert t(lambda x: {'k3': 'abc123'})({'k1': '123', 'k2': 'abc'}) == {'k3': 'abc123'}

    ###
    # regex
    ###

    # simple
    assert t( ("k1", r"hello (?P<name>\w+)") )({'k1': 'hello ding'}) == {'k1': 'hello ding', 'name': 'ding'}
    # multiple
    assert t( ("k1", r"(?i)(?P<word>[a-z]+)(?P<num>\d+)") )({'k1': 'aBc1234'}) == {'k1': 'aBc1234', 'word': 'aBc', 'num': '1234'}
    # not match
    assert t( ("k1", r"(?P<abc>\d+)") )({'k1': 'aBc1234'}) == {'k1': 'aBc1234', 'abc': '1234'}

    # full match
    assert t( ("k1", r".*?(?P<abc>\d+)") )({'k1': 'aBc1234'}) == {'k1': 'aBc1234', 'abc': '1234'}

    # regex multiple inputs
    assert t((["k1", 'k2'], r"(?P<abc>\d+)"))({'k1': 'abc123'}) == {'k1': 'abc123', 'abc': '123'}
    assert t((["k1", 'k2'], r"(?P<abc>\d+)"))({'k2': 'xyz334'}) == {'k2': 'xyz334', 'abc': '334'}

    # REGEX
    assert t( ("k1", REGEX(r"^(?P<abc>\d+)$") ))({'k1': 'aBc1234'}) == {'k1': 'aBc1234'}
    assert t( ("k1", REGEX(r"(?P<abc>\d+)") ))({'k1': 'aBc1234'}) == {'k1': 'aBc1234', 'abc': '1234'}

    # regex 3-tuple filed-string
    assert t(("k1", REGEX(r"\d+", "f1")))({'k1': '123 456'}) == {'k1': '123 456', 'f1': "123"}
    assert t(("k1", REGEX(r"\d+", ["f1", "f2"])))({'k1': '123 456'}) == {'k1': '123 456', 'f1': "123", "f2": "456"}
    assert t(("k1", r"\d+", ["f1", "f2"]))({'k1': '123 456'}) == {'k1': '123 456', 'f1': "123", "f2": "456"}
    assert t(("k1", r"(\w+) (\d+)", ["f1", "f2"]))({'k1': 'abc 123'}) == {'k1': 'abc 123', 'f1': "abc", "f2": "123"}

    # regex group - first match
    assert t(("k1", r"(\d+) (\d+)", ["f1", "f2"]))({'k1': '12 34 56 78'}) == {'k1': '12 34 56 78', 'f1': "12", "f2": "34"}

    # regex 3-tuple dict
    assert t(("k1", r"(\w+):(\d+)", {r"k_\1": r"v_\2"}))({'k1': 'abc:123 xyz:456'}) == {'k1': 'abc:123 xyz:456', 'k_abc': "v_123", "k_xyz": "v_456"}


def test_dispatch_transform():
    DISPATCH_LIST_data = [
        ({"data": "LTE_Information .+"}, {"__topic__": "etl_info"}),
        ({"data": "Status,.+"}, {"__topic__": "machine_status"}),
        ({"data": "System Reboot .+"}, {"__topic__": "reboot_event"}),
        ({"data": "Provision Firmware Download start.+"}, {"__topic__": "download"}),
        (True, {"__topic__": "unknown"})]

    e1 = {'data': 'LTE_Information 80,-82,17,4402010820E2DC5D,3750,-8'}
    e2 = {'data': 'Status,1.14% usr 0.00% sys,55464K,1,1,1,19 days 1 hours 09 minutes,7,7,7,7'}
    e3 = {'data': 'System Reboot [5]'}
    e4 = {'data': 'Provision Firmware Download start [J18V154.00_R2.67]'}

    e1_new = {'data': 'LTE_Information 80,-82,17,4402010820E2DC5D,3750,-8', "__topic__": "etl_info"}
    e2_new = {'data': 'Status,1.14% usr 0.00% sys,55464K,1,1,1,19 days 1 hours 09 minutes,7,7,7,7', "__topic__": "machine_status"}
    e3_new = {'data': 'System Reboot [5]', "__topic__": "reboot_event"}
    e4_new = {'data': 'Provision Firmware Download start [J18V154.00_R2.67]', "__topic__": "download"}

    assert dispatch_event(DISPATCH_LIST_data)(e1) == e1_new
    assert dispatch_event(DISPATCH_LIST_data)(e2) == e2_new
    assert dispatch_event(DISPATCH_LIST_data)(e3) == e3_new
    assert dispatch_event(DISPATCH_LIST_data)(e4) == e4_new

    TRANSFORM_LIST_data = [
        ({"data": "^LTE_Information "}, {"__topic__": "etl_info"}),
        ({"data": "^Status,"}, {"__topic__": "machine_status"}),
        ({"data": "^System Reboot "}, {"__topic__": "reboot_event"}),
        ({"data": "^Provision Firmware Download start"}, {"__topic__": "download"}),
        (True, {"event_type": "hello_data"})]

    e1_new = {'data': 'LTE_Information 80,-82,17,4402010820E2DC5D,3750,-8', "__topic__": "etl_info", "event_type": "hello_data"}
    e2_new = {'data': 'Status,1.14% usr 0.00% sys,55464K,1,1,1,19 days 1 hours 09 minutes,7,7,7,7', "__topic__": "machine_status", "event_type": "hello_data"}
    e3_new = {'data': 'System Reboot [5]', "__topic__": "reboot_event", "event_type": "hello_data"}
    e4_new = {'data': 'Provision Firmware Download start [J18V154.00_R2.67]', "__topic__": "download", "event_type": "hello_data"}

    assert transform_event(TRANSFORM_LIST_data)(e1) == e1_new
    assert transform_event(TRANSFORM_LIST_data)(e2) == e2_new
    assert transform_event(TRANSFORM_LIST_data)(e3) == e3_new
    assert transform_event(TRANSFORM_LIST_data)(e4) == e4_new


def test_meta():
    e1 = {'k1': "v1", 'k2': 'v2', 'x1': 'v3', 'x5': 'v4'}
    e2 = {"result": "Ok", "status": "400"}
    e3 = {"result": "Pass", "status": "200"}
    e4 = {"result": "failure", "status": "500"}
    e5 = {'__raw__': "some errors happen"}

    c1 = [{"result": r"(?i)ok|pass"}, {"status": lambda v: int(v) == 200}]
    c2 = [{"result": r"(?i)ok|pass", "status": lambda v: int(v) == 200}]
    c3 = lambda e: ('__raw__' in e and 'error' in e['__raw__'])

    assert keep_event(True)(e1) == e1
    assert keep_event(False)(e1) is None

    assert keep_event(c1)(e2) == e2
    assert keep_event(c1)(e3) == e3
    assert keep_event(c1)(e4) is None
    assert keep_event(c3)(e5) == e5

    assert drop_event(c1)(e2) is None
    assert drop_event(c1)(e3) is None
    assert drop_event(c1)(e4) == e4
    assert drop_event(c3)(e5) is None

    c1 = {'k1':'1', 'k2':'2', 'abc123': '3'}
    assert KEEP_F(r"\d+")(c1) == {}
    assert KEEP_F(r"\w+")(c1) == c1
    assert KEEP_F(r"k\d+")(c1) == {'k1':'1', 'k2':'2'}
    assert KEEP_F(["k1", "abc\d+"])(c1) == {'k1': '1', 'abc123': '3'}

    assert DROP_F(r"\d+")(c1) == c1
    assert DROP_F(r"\w+")(c1) == {}
    assert DROP_F(r"k\d+")(c1) == {'abc123': '3'}
    assert DROP_F(["k2", "abc\d+"])(c1) == {'k1': '1'}

    c1 = {'k1': '1', 'k2': '2', 'abc123': '3'}
    assert ALIAS({"k1": "k1_new", "k2": "k2_new"})(c1) == {'k1_new': '1', 'k2_new': '2', 'abc123': '3'}
    assert RENAME({"k4": "k4_new"})(c1) == c1


def verify_parse_result(ret, expect):
    assert len(ret) == len(expect)
    for i, (no, name) in enumerate(ret):
        no2, name2 = expect[i]
        assert no == no2
        if name2 != '*':
            assert name2 in str(name), Exception(ret, expect)


def test_parse():
    import config1
    import config2
    import config3

    expect = [[4, '*'], [6, '*'], [9, '*'], [11, '*'], [13, '*'], [15, '*'], [18, 'sls_eu_my_logic'], [23, '*'],
              [30, 'sls_eu_parse_data'], [34, '*']]
    verify_parse_result(ConfigParser(config1).parse(), expect)

    assert ConfigParser(config2).parse() == []

    expect = [[4, 'keep_event'], [8, 'drop_event'], [12, 'keep_fields'], [17, 'dispatch_event'], [28, 'transform_event'], [37, 'drop_fields'], [39, 'rename_fields']]
    verify_parse_result(ConfigParser(config3).parse(), expect)

    import data1_test1
    import data1_test2
    expect = [[3, 'drop_fields'], [7, 'dispatch_event'], [16, 'transform_event']]
    verify_parse_result(ConfigParser(data1_test1).parse(), expect)

    expect = [[5, 'drop_fields'], [7, 'dispatch_event'], [16, 'transform_event']]
    verify_parse_result(ConfigParser(data1_test2).parse(), expect)

    import data2_test1
    import data2_test3
    import data2_test2
    expect = [[4, 'keep_event'], [7, 'drop_event'], [11, 'keep_fields'], [16, 'dispatch_event'], [27, 'transform_event'], [36, 'drop_fields'], [37, 'rename_fields']]
    verify_parse_result(ConfigParser(data2_test1).parse(), expect)

    expect = [[4, 'keep_event'], [7, 'drop_event'], [11, 'keep_fields'], [14, 'sls_en_dispatch'], [35, 'transform_event'], [40, 'sls_eu_windows'], [44, 'sls_en_windows'], [49, 'sls_eu_anoymouse_ip'], [54, 'drop_fields'], [55, 'rename_fields']]
    verify_parse_result(ConfigParser(data2_test2).parse(), expect)

    expect = [[4, 'keep_event'], [8, 'drop_event'], [12, 'keep_fields'], [17, 'dispatch_event'], [28, 'transform_event'], [37, 'drop_fields'], [38, 'drop_fields'], [40, 'rename_fields']]
    verify_parse_result(ConfigParser(data2_test3).parse(), expect)


def test_runner():
    e = {'k1': 'xyz', 'k2': 'abc'}
    assert Runner('./config2.py')(e) == e


def verify_case(config, data, result):
    basedir = os.path.dirname(os.path.abspath(__file__))

    lines = open(os.sep.join([basedir, data])).read().split('\n')
    results = open(os.sep.join([basedir, result])).read().split('\n')
    removed_lines = 0
    added_lines = 0
    run = Runner(config)
    for i, line in enumerate(lines):
        if line:
            e = json.loads(line)
            ret = run(e)
            if ret is None:  # removed line
                removed_lines += 1

            next_result_line = i - removed_lines + added_lines
            if isinstance(ret, (tuple, list)):
                for e in ret:
                    r = json.loads(results[next_result_line])
                    assert e == r, Exception(i, line, ret, next_result_line, r)
                    next_result_line += 1
                added_lines += len(ret) - 1
            elif ret:
                # print(json.dumps(ret))
                r = json.loads(results[next_result_line])
                assert ret == r, Exception(i, line, ret, (next_result_line), r)

    assert len(results) == len(lines)-removed_lines+added_lines, Exception(len(results), len(lines), removed_lines, added_lines)


def test_module():
    import data1_test1
    import data1_test2
    import data1_test3
    verify_case(data1_test1, './data1.txt', './data1_test1_result.txt')
    verify_case(data1_test2, './data1.txt', './data1_test2_result.txt')
    verify_case(data1_test3, './data1.txt', './data1_test3_result.txt')

    import data2_test1
    import data2_test3
    import data2_test2
    verify_case(data2_test1, './data2.txt', './data2_test1_result.txt')
    verify_case(data2_test2, './data2.txt', './data2_test1_result.txt')
    verify_case(data2_test3, './data2.txt', './data2_test1_result.txt')

    import data3_test1
    verify_case(data3_test1, './data3.txt', './data3_test1_result.txt')

    import data4_test1
    verify_case(data4_test1, './data4.txt', './data4_test1_result.txt')


def test_csv():
    # sep
    assert t( ("data", CSV(r"city,pop,province") ))({'data': 'nj,800,js'})  == {'province': 'js', 'city': 'nj', 'data': 'nj,800,js', 'pop': '800'}
    assert t(("data", CSV(r"city, pop, province", sep='#')))({'data': 'nj#800#js'}) == {'province': 'js', 'city': 'nj', 'data': 'nj#800#js', 'pop': '800'}

    # config
    assert t( ("data", CSV(['city', 'pop', 'province']) ))({'data': 'nj, 800, js'})  == {'province': 'js', 'city': 'nj', 'data': 'nj, 800, js', 'pop': '800'}

    # lstrip
    assert t( ("data", CSV(r"city, pop, province") ))({'data': 'nj, 800, js'})  == {'province': 'js', 'city': 'nj', 'data': 'nj, 800, js', 'pop': '800'}
    assert t( ("data", CSV(r"city, pop, province", lstrip=False) ))({'data': 'nj, 800, js'})  == {'province': ' js', 'city': 'nj', 'data': 'nj, 800, js', 'pop': ' 800'}

    # quote
    assert t( ("data", CSV(r"city, pop, province") ))({'data': '"nj", "800", "js"'})  == {'province': 'js', 'city': 'nj', 'data': '"nj", "800", "js"', 'pop': '800'}
    assert t( ("data", CSV(r"city, pop, province") ))({'data': '"nj", "800", "jiang, su"'})  == {'province': 'jiang, su', 'city': 'nj', 'data': '"nj", "800", "jiang, su"', 'pop': '800'}
    assert t( ("data", CSV(r"city, pop, province") ))({'data': '"nj", "800", "jiang"" su"'})  == {'province': 'jiang" su', 'city': 'nj', 'data': '"nj", "800", "jiang"" su"', 'pop': '800'}
    assert t( ("data", CSV(r"city, pop, province", quote='|') ))({'data': '|nj|, |800|, |jiang, su|'})  == {'province': 'jiang, su', 'city': 'nj', 'data': '|nj|, |800|, |jiang, su|', 'pop': '800'}

    # restrict
    assert t(("data", CSV(r"city, pop, province")))({'data': 'nj,800,js,gudu'})  == {'province': 'js', 'city': 'nj', 'data': 'nj,800,js,gudu', 'pop': '800'}
    assert t(("data", CSV(r"city, pop, province", restrict=True)))({'data': 'nj,800,js,gudu'})  == {'data': 'nj,800,js,gudu'}
    assert t(("data", CSV(r"city, pop, province", restrict=True)))({'data': 'nj,800'})  == {'data': 'nj,800'}

    # TSV
    assert t( ("data", TSV(r"city,pop,province") ))({'data': 'nj\t800\tjs'})  == {'province': 'js', 'city': 'nj', 'data': 'nj\t800\tjs', 'pop': '800'}

    # PSV
    assert t( ("data", PSV(r"city,pop,province") ))({'data': 'nj|800|js'})  == {'province': 'js', 'city': 'nj', 'data': 'nj|800|js', 'pop': '800'}


def test_lookup_dict():
    # no field
    assert t( ("pro", LOOKUP({"1": "TCP", "2": "UDP", "3": "HTTP", "*": "Unknown"}, "protocol") ) )({'data': '123'})  == {'data': '123'}

    # match
    assert t( ("pro", LOOKUP({"1": "TCP", "2": "UDP", "3": "HTTP", "*": "Unknown"}, "protocol") ) )({'data': '123', "pro": "1"})  == {'data': '123', "pro": "1", "protocol": "TCP"}
    assert t( ("pro", LOOKUP({"1": "TCP", "2": "UDP", "3": "HTTP", "*": "Unknown"}, "protocol") ) )({'data': '123', "pro": "3"})  == {'data': '123', "pro": "3", "protocol": "HTTP"}

    # match - default
    assert t( ("pro", LOOKUP({"1": "TCP", "2": "UDP", "3": "HTTP", "*": "Unknown"}, "protocol") ) )({'data': '123', "pro": "4"})  == {'data': '123', "pro": "4", "protocol": "Unknown"}
    assert t( ("pro", LOOKUP({"1": "TCP", "2": "UDP", "3": "HTTP"}, "protocol") ) )({'data': '123', "pro": "4"})  == {'data': '123', "pro": "4"}

    # case insensitive
    assert t( ("pro", LOOKUP({"http": "tcp", "dns": "udp", "https": "tcp"}, "type") ) )({'data': '123', "pro": "http"})  == {'data': '123', "pro": "http", "type": "tcp"}
    assert t( ("pro", LOOKUP({"http": "tcp", "dns": "udp", "https": "tcp"}, "type") ) )({'data': '123', "pro": "Http"})  == {'data': '123', "pro": "Http", "type": "tcp"}

    # case sensitive
    assert t(("pro", LOOKUP({"http": "tcp", "dns": "udp", "https": "tcp", "*": "Unknown"}, "type", case_insensitive=False)))({'data': '123', "pro": "Http"}) == {'data': '123', "pro": "Http", "type": "Unknown"}
    assert t(("pro", LOOKUP({"http": "tcp", "dns": "udp", "https": "tcp", "*": "Unknown"}, "type", case_insensitive=False)))({'data': '123', "pro": "dns"}) == {'data': '123', "pro": "dns", "type": "udp"}

    # multiple inputs
    assert t((["pro", "protocol"], LOOKUP({"http": "tcp", "dns": "udp", "https": "tcp"}, "type")))({'data': '123', "pro": "http"}) == {'data': '123', "pro": "http", "type": "tcp"}
    assert t((["pro", "protocol"], LOOKUP({"http": "tcp", "dns": "udp", "https": "tcp"}, "type")))({'data': '123', "protocol": "http"}) == {'data': '123', "protocol": "http", "type": "tcp"}
    assert t((["pro", "protocol"], LOOKUP({"http": "tcp", "dns": "udp", "https": "tcp"}, "type")))({'data': '123', "pro": "dns", "protocol": "http"}) == {'data': '123', "pro": "dns", "protocol": "http", "type": "tcp"}


import atexit
_tmp_files = set()


def _del_csv():
    for x in _tmp_files:
        os.unlink(x)


def _pre_csv(content, suffix=None):
    suffix = suffix or "{0}_{1}".format(time(), randint(1, 1000000))
    file_path = './tmp_test_lookup_csv_{0}.csv'.format(suffix)

    with open(file_path, "w") as f:
        f.write(content)

    _tmp_files.add(file_path)
    return file_path


atexit.register(_del_csv)


def test_lookup_load_csv():
    #########
    # CSV standard reading/mapping

    # prepare
    csv_path = _pre_csv("city,pop,province\nnj,800,js\nsh,2000,sh", 1)

    # output 1 fields
    assert t( ("city", LOOKUP(csv_path, "province") ))({'data': '123', 'city': 'nj'})  == {'data': '123', 'city': 'nj', 'province': 'js'}

    # output 2 fields
    assert t( ("city", LOOKUP(csv_path, ["province", "pop"]) ))({'data': '123', 'city': 'nj'})  == {'data': '123', 'city': 'nj', 'province': 'js', 'pop': '800'}

    # cache
    csv_path = _pre_csv("nothing just empty to re-use cached version", 1)
    assert t( ("city", LOOKUP(csv_path, ["province", "pop"]) ))({'data': '123', 'city': 'nj'})  == {'data': '123', 'city': 'nj', 'province': 'js', 'pop': '800'}

    # file type
    csv_path = 'file://' + _pre_csv("city,pop,province\nnj,800,js\nsh,2000,sh")
    assert t( ("city", LOOKUP(csv_path, ["province", "pop"]) ))({'data': '123', 'city': 'nj'})  == {'data': '123', 'city': 'nj', 'province': 'js', 'pop': '800'}

    # sep
    csv_path = _pre_csv("city#pop#province\nnj#800#js\nsh#2000#sh")
    assert t( ("city", LOOKUP(csv_path, ["province", "pop"], sep='#') ))({'data': '123', 'city': 'nj'})  == {'data': '123', 'city': 'nj', 'province': 'js', 'pop': '800'}

    # lstrip
    csv_path = _pre_csv("city,pop,province\nnj, 800, js\nsh, 2000, sh")
    assert t( ("city", LOOKUP(csv_path, ["province", "pop"], lstrip=False) ))({'data': '123', 'city': 'nj'})  == {'data': '123', 'city': 'nj', 'province': ' js', 'pop': ' 800'}

    # quote
    csv_path = _pre_csv('city,pop,province\n  "nj",  "800",  "js"\n"shang hai",2000,  "SHANG,HAI"')
    assert t( ("city", LOOKUP(csv_path, ["province", "pop"]) ))({'data': '123', 'city': 'nj'})  == {'data': '123', 'city': 'nj', 'province': 'js', 'pop': '800'}
    assert t( ("city", LOOKUP(csv_path, ["province", "pop"]) ))({'data': '123', 'city': 'shang hai'})  == {'data': '123', 'city': 'shang hai', 'province': 'SHANG,HAI', 'pop': '2000'}

    # quote in header
    csv_path = _pre_csv('city,"city,pop","city,province"\n"nj","800","js"\n"shang hai",2000,"SHANG,HAI"')
    assert t( ("city", LOOKUP(csv_path, ["city,province", "city,pop"]) ))({'data': '123', 'city': 'nj'})  == {'data': '123', 'city': 'nj', 'city,province': 'js', 'city,pop': '800'}
    assert t( ("city", LOOKUP(csv_path, ["city,province", "city,pop"]) ))({'data': '123', 'city': 'shang hai'})  == {'data': '123', 'city': 'shang hai', 'city,province': 'SHANG,HAI', 'city,pop': '2000'}

    # quote - custom
    csv_path = _pre_csv('city,pop,province\n|nj|,|800|,|js|\n|shang hai|,2000,|SHANG,HAI|')
    assert t( ("city", LOOKUP(csv_path, ["province", "pop"], quote='|') ))({'data': '123', 'city': 'nj'})  == {'data': '123', 'city': 'nj', 'province': 'js', 'pop': '800'}
    assert t( ("city", LOOKUP(csv_path, ["province", "pop"], quote='|') ))({'data': '123', 'city': 'shang hai'})  == {'data': '123', 'city': 'shang hai', 'province': 'SHANG,HAI', 'pop': '2000'}


def test_lookup_mapping():

    #########
    # lookup mapping
    csv_path = _pre_csv("city,pop,province\nnj,800,js\nsh,2000,sh")

    # no field
    assert t( ("city", LOOKUP(csv_path, "province") ) )({'data': '123'})  == {'data': '123'}
    assert t( (["city", "province"], LOOKUP(csv_path, "pop") ) )({'data': '123', 'city': 'sh'})  == {'data': '123', 'city': 'sh'}

    # match - no match
    assert t( ("city", LOOKUP(csv_path, "province") ) )({'data': '123', "city": "bj"})  == {'data': '123', "city": "bj"}

    csv_path = _pre_csv("pro,type\nhttp, tcp\ndns, udp\nhttps, tcp")

    # case insensitive
    assert t( ("pro", LOOKUP(csv_path, "type") ) )({'data': '123', "pro": "http"})  == {'data': '123', "pro": "http", "type": "tcp"}
    assert t( ("pro", LOOKUP(csv_path, "type") ) )({'data': '123', "pro": "Http"})  == {'data': '123', "pro": "Http", "type": "tcp"}

    csv_path = _pre_csv("pro,type\nhttp, tcp\ndns, udp\nhttps, tcp\n*, Unknown")

    # case sensitive
    assert t(("pro", LOOKUP(csv_path, "type", case_insensitive=False)))({'data': '123', "pro": "Http"}) == {'data': '123', "pro": "Http", "type": "Unknown"}
    assert t(("pro", LOOKUP(csv_path, "type", case_insensitive=False)))({'data': '123', "pro": "dns"}) == {'data': '123', "pro": "dns", "type": "udp"}


    csv_path = _pre_csv("city,province,pop,type\nnj,js,800,abc\nnj,sd,900,xyz\nsh,sh,2000,zzz")

    # multiple inputs/outputs
    assert t( (["city", "province"], LOOKUP(csv_path, "pop") ) )({'data': '123', 'city': 'sh', 'province': 'sh'})  == {'data': '123', 'city': 'sh', 'province': 'sh', 'pop': '2000'}
    assert t( (["city", "province"], LOOKUP(csv_path, ["pop", "type"]) ) )({'data': '123', 'city': 'nj', 'province': 'sd'})  == {'data': '123', 'city': 'nj', 'province': 'sd', 'pop': '900', 'type': 'xyz'}

    # alias - input
    assert t( ([("ct", "city"), "province"], LOOKUP(csv_path, "pop") ) )({'data': '123', 'ct': 'sh', 'province': 'sh'})  == {'data': '123', 'ct': 'sh', 'province': 'sh', 'pop': '2000'}
    assert t( ([("ct", "city"), ("prv", "province")], LOOKUP(csv_path, "pop") ) )({'data': '123', 'ct': 'sh', 'prv': 'sh'})  == {'data': '123', 'ct': 'sh', 'prv': 'sh', 'pop': '2000'}

    # alias - output
    assert t( (["city", "province"], LOOKUP(csv_path, [("pop", "population"), ("type", "city_type") ]) ) )({'data': '123', 'city': 'sh', 'province': 'sh'})  == {'data': '123', 'city': 'sh', 'province': 'sh', 'population': '2000', 'city_type': 'zzz'}
    assert t( ([("ct", "city"), ("prv", "province")], LOOKUP(csv_path, [("pop", "population")] ) ))({'data': '123', 'ct': 'sh', 'prv': 'sh'})  == {'data': '123', 'ct': 'sh', 'prv': 'sh', 'population': '2000'}

    # star match
    csv_path = _pre_csv("c1,c2,d1,d2\na,b,1,2\na,c,2,3\na,e,4,6\na,*,10,11\nb,*,20,21\n*,*,0,0")
    assert t( (["c1", "c2"], LOOKUP(csv_path, ["d1", "d2"]) ) )({'data': '123', 'c1': 'a', 'c2': 'x'})  == {'data': '123', 'c1': 'a', 'c2': 'x', 'd1': '10', 'd2': '11'}
    assert t( (["c1", "c2"], LOOKUP(csv_path, ["d1", "d2"]) ) )({'data': '123', 'c1': 'b', 'c2': 'x'})  == {'data': '123', 'c1': 'b', 'c2': 'x', 'd1': '20', 'd2': '21'}
    assert t( (["c1", "c2"], LOOKUP(csv_path, ["d1", "d2"]) ) )({'data': '123', 'c1': 'c', 'c2': 'v'})  == {'data': '123', 'c1': 'c', 'c2': 'v', 'd1': '0', 'd2': '0'}


def test_kv():
    # verify the KV extract pattern match
    d1 = {"data": "i=c1, k1=v1,k2=v2 k3=v3"}
    assert t( ("data", KV) )(d1) == {'i': 'c1', 'k2': 'v2', 'k1': 'v1', 'k3': 'v3', 'data': 'i=c1, k1=v1,k2=v2 k3=v3'}

    d2 = {"data": 'i=c2, k1=" v 1 ", k2="v 2" k3="~!@#=`;.>"'}
    assert t(("data", KV))(d2) == {'i': 'c2', 'k2': 'v 2', 'k1': 'v 1', 'k3': '~!@#=`;.>', 'data': 'i=c2, k1=" v 1 ", k2="v 2" k3="~!@#=`;.>"'}

    # multi-bytes check
    if six.PY2:
        d3 = {"data": u'i=c3, k1=你好 k2=他们'.encode('utf8')}
        assert t(("data", KV))(d3) == {'i': 'c3', 'k2': u'他们'.encode('utf8'), 'k1': u'你好'.encode('utf8'), "data": u'i=c3, k1=你好 k2=他们'.encode('utf8')}

        d4 = {"data": u'i=c4, 姓名=小明 年龄=中文 '.encode('utf8')}
        assert t(("data", KV))(d4) == {'i': 'c4', u'姓名'.encode('utf8'): u'小明'.encode('utf8'), u'年龄'.encode('utf8'): u'中文'.encode('utf8'), "data": u'i=c4, 姓名=小明 年龄=中文 '.encode('utf8')}

        d5 = {"data": u'i=c5, 姓名="小明" 年龄="中文" '.encode('utf8')}
        assert t(("data", KV))(d5) == {'i': 'c5', u'姓名'.encode('utf8'): u'小明'.encode('utf8'), u'年龄'.encode('utf8'): u'中文'.encode('utf8'), "data": u'i=c5, 姓名="小明" 年龄="中文" '.encode('utf8')}

        d6 = {"data": u'i=c6, 姓名=小明 年龄=中文'}
        assert t(("data", KV))(d6) == {'i': 'c6', u'姓名'.encode('utf8'): u'小明'.encode('utf8'), u'年龄'.encode('utf8'): u'中文'.encode('utf8'), "data": u'i=c6, 姓名=小明 年龄=中文'}

        d7 = {"data": u'i=c7, 姓名="小明" 年龄=中文 '}
        assert t(("data", KV))(d7) == {'i': 'c7', u'姓名'.encode('utf8'): u'小明'.encode('utf8'), u'年龄'.encode('utf8'): u'中文'.encode('utf8'), "data": u'i=c7, 姓名="小明" 年龄=中文 '}
    else:
        d3 = {"data": u'i=c3, k1=你好 k2=他们'.encode('utf8')}
        assert t(("data", KV))(d3) == {'i': 'c3', 'k2': u'他们', 'k1': u'你好', "data": u'i=c3, k1=你好 k2=他们'.encode('utf8')}

        d4 = {"data": u'i=c4, 姓名=小明 年龄=中文 '.encode('utf8')}
        assert t(("data", KV))(d4) == {'i': 'c4', u'姓名': u'小明', u'年龄': u'中文', "data": u'i=c4, 姓名=小明 年龄=中文 '.encode('utf8')}

        d5 = {"data": u'i=c5, 姓名="小明" 年龄="中文" '.encode('utf8')}
        assert t(("data", KV))(d5) == {'i': 'c5', u'姓名': u'小明', u'年龄': u'中文', "data": u'i=c5, 姓名="小明" 年龄="中文" '.encode('utf8')}

        d6 = {"data": u'i=c6, 姓名=小明 年龄=中文'}
        assert t(("data", KV))(d6) == {'i': 'c6', u'姓名': u'小明', u'年龄': u'中文', "data": u'i=c6, 姓名=小明 年龄=中文'}

        d7 = {"data": u'i=c7, 姓名="小明" 年龄=中文 '}
        assert t(("data", KV))(d7) == {'i': 'c7', u'姓名': u'小明', u'年龄': u'中文', "data": u'i=c7, 姓名="小明" 年龄=中文 '}


    # new line in value
    d8 = {"data": """i=c8, k1="hello
    world" k2="good
    morning"
    """}
    assert t(("data", KV))(d8) == {'i': 'c8', 'k2': 'good\n    morning', 'k1': 'hello\n    world', 'data': 'i=c8, k1="hello\n    world" k2="good\n    morning"\n    '}

    ################
    ## Options

    # sep-regex
    d9 = {"data": "i=c9 k1:v1, k2=v2"}
    assert t(("data", KV(sep='(?::|=)')))(d9) == {'k2': 'v2', 'k1': 'v1', 'i': 'c9', 'data': 'i=c9 k1:v1, k2=v2'}

    # quote
    d10 = {"data": "i=c10 a='k1=k2;k2=k3'"}
    assert t(("data", KV(quote="'")))(d10) == {'i': 'c10', 'a': 'k1=k2;k2=k3', 'data': "i=c10 a='k1=k2;k2=k3'"}

    # prefix/suffix
    d11 = {"data": "i=c11, k1=v1,k2=v2 k3=v3"}
    assert t( ("data", KV(prefix="d_", suffix="_e")) )(d11) == {'d_i_e': 'c11', 'd_k3_e': 'v3', 'd_k2_e': 'v2', 'data': 'i=c11, k1=v1,k2=v2 k3=v3', 'd_k1_e': 'v1'}

    # multiple inputs
    d12 = {"data1": "i=c12, k1=v1", "data2": "k2=v2 k3=v3", "data3": "k4=v4"}
    assert t( (["data1", "data2"], KV) )(d12) == {'k3': 'v3', 'k2': 'v2', 'k1': 'v1', 'i': 'c12', 'data1': 'i=c12, k1=v1', 'data2': 'k2=v2 k3=v3', "data3": "k4=v4"}

    #############
    # KV_F

    d13 = {"data1": "i=c13, k1=v1", "data2": "k2=v2 k3=v3", "data3": "k4=v4"}
    assert KV_F(["data1", "data2"])(d13) == {'k3': 'v3', 'k2': 'v2', 'k1': 'v1', 'i': 'c13', 'data1': 'i=c13, k1=v1', 'data3': 'k4=v4', 'data2': 'k2=v2 k3=v3'}

    d14 = {"data1": "i=c14, k1=v1", "data2": "k2=v2 k3=v3", "data3": "k4=v4"}
    assert KV_F(r'data2')(d14) == {'k3': 'v3', 'k2': 'v2', 'data1': 'i=c14, k1=v1', 'data3': 'k4=v4', 'data2': 'k2=v2 k3=v3'}


def test_split():
    # verify basic SPLIT w/o parameters
    d1 = {"i": "t1", "data": "1,2,3"}
    assert t( ("data", SPLIT) )(d1) == [{"i": "t1", "data": "1"}, {"i": "t1", "data": "2"}, {"i": "t1", "data": "3"}]

    # json list
    d2 = {"i": "t1", "data": "[1,2,3]"}
    assert t( ("data", SPLIT) )(d2) == [{"i": "t1", "data": "1"}, {"i": "t1", "data": "2"}, {"i": "t1", "data": "3"}]

    # json list
    d3 = {"i": "t1", "data": '["a", "b", "c"]'}
    assert t( ("data", SPLIT) )(d3) == [{"i": "t1", "data": 'a'}, {"i": "t1", "data": "b"}, {"i": "t1", "data": "c"}]

    # sep
    assert t(("data", SPLIT(sep='#')))({"i": "t1", 'data': 'nj#800#js'}) == [{"i": "t1", 'data': 'nj'},{"i": "t1", 'data': '800'},{"i": "t1", 'data': 'js'}]

    # lstrip
    assert t( ("data", SPLIT ))({"i": "t1", 'data': 'nj, 800, js'})  == [{"i": "t1", 'data': 'nj'},{"i": "t1", 'data': '800'},{"i": "t1", 'data': 'js'}]
    assert t( ("data", SPLIT(lstrip=False) ))({"i": "t1", 'data': 'nj, 800, js'})  == [{"i": "t1", 'data': 'nj'},{"i": "t1", 'data': ' 800'},{"i": "t1", 'data': ' js'}]

    # quote
    assert t( ("data", SPLIT ))({"i": "t1", 'data': '"nj", "800", "js"'})  == [{"i": "t1", 'data': 'nj'}, {"i": "t1",'data': '800'}, {"i": "t1",'data': 'js'} ]
    assert t( ("data", SPLIT ))({"i": "t1", 'data': '"nj", "800", "jiang, su"'})  == [{"i": "t1", 'data': 'nj'}, {"i": "t1",'data': '800'}, {"i": "t1",'data': 'jiang, su'} ]
    assert t( ("data", SPLIT ))({"i": "t1", 'data': '"nj", "800", "jiang"" su"'})  == [{"i": "t1", 'data': 'nj'}, {"i": "t1",'data': '800'}, {"i": "t1",'data': 'jiang" su'} ]
    assert t( ("data", SPLIT(quote='|') ))({"i": "t1", 'data': '|nj|, |800|, |jiang, su|'})  == [{"i": "t1", 'data': 'nj'}, {"i": "t1",'data': '800'}, {"i": "t1",'data': 'jiang, su'} ]

    # output fields
    assert t( ("data", SPLIT(output="v")) )({"data": "[1,2,3]"}) == [{"v": "1", "data": "[1,2,3]"}, {"v": "2", "data": "[1,2,3]"}, {"v": "3", "data": "[1,2,3]"}]


def _get_file_content(path):

    basedir = os.path.dirname(os.path.abspath(__file__))

    with open(os.sep.join([basedir, path])) as f:
        return f.read()


def test_split_filter():
    d1 = {'i': '1', 'data': _get_file_content('json_data/CVE-2013-0169.json')}
    # jmes filter output as list
    jmes = 'cve.affects.vendor.vendor_data[*].product.product_data[*].product_name[]'
    assert t( ("data", SPLIT(jmes=jmes, output='data')) )(d1) == [{"i": "1", 'data':  "openssl"}, {"i": "1", 'data':  "openjdk"}, {"i": "1", 'data':  "polarssl"}]

    d1 = {'i': '1', 'data': _get_file_content('json_data/CVE-2013-0169.json')}
    # jmes filter output as string with dot
    jmes = 'cve.affects.vendor.vendor_data[1].product.product_data[0].version.version_data[1].version_value'
    # print(t( ("data", SPLIT(jmes=jmes, sep='.', output='data')) )(d1))
    assert t( ("data", SPLIT(jmes=jmes, sep='.', output='data')) )(d1) == [{"i": "1", 'data':  "1"}, {"i": "1", 'data':  "6"}, {"i": "1", 'data':  "0"}]

    d1 = {'i': '1', 'data': _get_file_content('json_data/CVE-2013-0169.json')}
    # jmes filter output a simple string
    jmes = 'cve.CVE_data_meta.ID'
    assert t(("data", SPLIT(jmes=jmes, output='data')))(d1) == {"i": "1", 'data': "CVE-2013-0169"}

    d1 = {'i': '1', 'data': _get_file_content('json_data/CVE-2013-0169.json')}
    jmes = 'cve.affects.vendor.vendor_data[*].product.product_data[]'
    # print(t(("data", SPLIT(jmes=jmes, output='data')))(d1))
    assert t(("data", SPLIT(jmes=jmes, output='data')))(d1) == [{'i': '1', 'data': '{"product_name": "openssl", "version": {"version_data": [{"version_value": "*"}, {"version_value": "0.9.8"}, {"version_value": "0.9.8a"}, {"version_value": "0.9.8b"}, {"version_value": "0.9.8c"}, {"version_value": "0.9.8d"}, {"version_value": "0.9.8f"}, {"version_value": "0.9.8g"}]}}'}, {'i': '1', 'data': '{"product_name": "openjdk", "version": {"version_data": [{"version_value": "-"}, {"version_value": "1.6.0"}, {"version_value": "1.7.0"}]}}'}, {'i': '1', 'data': '{"product_name": "polarssl", "version": {"version_data": [{"version_value": "0.10.0"}, {"version_value": "0.10.1"}, {"version_value": "0.11.0"}]}}'}]


def test_json():
    # jmes filter
    d1 = {'i': '1', 'data': _get_file_content('json_data/CVE-2013-0169.json')}
    jmes = 'join(`,`,cve.affects.vendor.vendor_data[*].product.product_data[*].product_name[])'
    assert t( ("data", JSON(jmes=jmes, output='data')) )(d1) == {"i": "1", 'data':  "openssl,openjdk,polarssl"}

    # jmes filter - real match, empty result
    d1 = {'i': '1', 'data': '{"f1": ""}'}
    jmes = 'f1'
    assert t( ("data", JSON(jmes=jmes, output='data')) )(d1) == {'i': '1', 'data': ''}

    # jmes filter 1 option: jmes_ignore_none
    d1 = {'i': '1', 'data': '{"f1": [1,2,3]}'}
    jmes = 'f1[4]'
    assert t( ("data", JSON(jmes=jmes, output='data')) )(d1) == {'i': '1', 'data': '{"f1": [1,2,3]}'}
    assert t(("data", JSON(jmes=jmes, output='data', jmes_ignore_none=False)))(d1) == {'i': '1', 'data': ''}

    # jmes filter 2 option: jmes_ignore_none
    d1 = {'i': '1', 'data': '{"f1": "123"}'}
    jmes = 'f2'
    assert t( ("data", JSON(jmes=jmes, output='data')) )(d1) == {'i': '1', 'data': '{"f1": "123"}'}
    assert t(("data", JSON(jmes=jmes, output='data', jmes_ignore_none=False)))(d1) == {'i': '1', 'data': ''}

    # jmes filter - output
    d1 = {"data": """{"k1": 100, "k2": 200}"""}
    assert t( ("data", JSON(jmes='k2', output='k3')) )(d1) == {"data": """{"k1": 100, "k2": 200}""", 'k3': '200'}

    # auto expand
    d1 = {"data": """{"k1": 100, "k2": 200}"""}
    assert t( ("data", JSON) )(d1) == {"data": """{"k1": 100, "k2": 200}""", "k1": "100", "k2": "200"}

    # extract - deep - all depth
    d2 = {"data": """{"k1": 100, "k2": {"k3": 200, "k4": {"k5": 300} } }"""}
    assert t(("data", JSON))(d2) == {'data': '{"k1": 100, "k2": {"k3": 200, "k4": {"k5": 300} } }', 'k1': '100', 'k3': '200', 'k5': '300'}

    # extract - deep - level-1
    d2 = {"data": """{"k1": 100, "k2": {"k21": 200} }"""}
    assert t( ("data", JSON(depth=1)) )(d2) == {"data": """{"k1": 100, "k2": {"k21": 200} }""", "k1": "100", "k2": '{"k21": 200}'}

    # extract - deep - level-2
    d2 = {"data": """{"k1": 100, "k2": {"k3": 200, "k4": {"k5": 300} } }"""}
    assert t(("data", JSON(depth=2)))(d2) == {'data': '{"k1": 100, "k2": {"k3": 200, "k4": {"k5": 300} } }', 'k1': '100', 'k3': '200', 'k4': '{"k5": 300}'}

    # extract - deep - format - full
    d2 = {"data": """{"k1": 100, "k2": {"k3": 200, "k4": {"k5": 300} } }"""}
    assert t(("data", JSON(fmt='full')))(d2) == {'data': '{"k1": 100, "k2": {"k3": 200, "k4": {"k5": 300} } }', 'data.k1': '100', 'data.k2.k3': '200', 'data.k2.k4.k5': '300'}

    # extract - deep - format - parent
    d2 = {"data": """{"k1": 100, "k2": {"k3": 200, "k4": {"k5": 300} } }"""}
    assert t(("data", JSON(fmt='parent')))(d2) == {'data': '{"k1": 100, "k2": {"k3": 200, "k4": {"k5": 300} } }', 'data.k1': '100', 'k2.k3': '200', 'k4.k5': '300'}

    # extract - deep - format - root
    d2 = {"data": """{"k1": 100, "k2": {"k3": 200, "k4": {"k5": 300} } }"""}
    assert t(("data", JSON(fmt='root')))(d2) == {'data': '{"k1": 100, "k2": {"k3": 200, "k4": {"k5": 300} } }', 'data.k1': '100', 'data.k3': '200', 'data.k5': '300'}

    # extract - deep - format - custom
    d2 = {"data": """{"k1": 100, "k2": {"k3": 200, "k4": {"k5": 300} } }"""}
    assert t(("data", JSON(fmt='root')))(d2) == {'data': '{"k1": 100, "k2": {"k3": 200, "k4": {"k5": 300} } }', 'data.k1': '100', 'data.k3': '200', 'data.k5': '300'}

    # extract - array
    d3 = {"data": """[1,2,3]"""}
    assert t( ("data", JSON(expand_array=False)) )(d3) == {"data": """[1, 2, 3]"""}

    # # extract - array - expand
    # d3 = {"data": """[1,2,3]"""}
    # print( t( ("data", JSON) )(d3)  )
    # assert t( ("data", JSON) )(d3) == {"data": """[1, 2, 3]"""}

    # jmes - expand
    d1 = {'i': '1', 'data': _get_file_content('json_data/simple_data.json')}
    jmes = 'cve.CVE_data_meta'
    assert t( ("data", JSON(jmes=jmes, output='data', expand=True)) )(d1) == {'i': '1', 'data': '{"ASSIGNER": "cve@mitre.org", "ID": "CVE-2013-0169"}', 'ASSIGNER': 'cve@mitre.org', 'ID': 'CVE-2013-0169'}

    # expand prefix/suffix
    d1 = {"data": """{"k1": 100, "k2": 200}"""}
    assert t( ("data", JSON(prefix="data_", suffix="_end")) )(d1) == {'data': '{"k1": 100, "k2": 200}', 'data_k1_end': '100', 'data_k2_end': '200'}





# test_json()
# exit(1)

test_condition()
test_regex()
test_csv()
test_lookup_dict()
test_lookup_load_csv()
test_lookup_mapping()
test_kv()
test_split()
test_split_filter()
test_json()
test_dispatch_transform()
test_meta()
test_parse()
test_runner()
test_module()



