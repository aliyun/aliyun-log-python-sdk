
from aliyun.log.etl_core import *
from aliyun.log.etl_core.config_parser import ConfigParser
from aliyun.log.etl_core.runner import Runner
import json
import os

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
        ({"data": "^LTE_Information "}, {"__topic__": "etl_info"}),
        ({"data": "^Status,"}, {"__topic__": "machine_status"}),
        ({"data": "^System Reboot "}, {"__topic__": "reboot_event"}),
        ({"data": "^Provision Firmware Download start"}, {"__topic__": "download"}),
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
    run = Runner(config)
    for i, line in enumerate(lines):
        if line:
            e = json.loads(line)
            ret = run(e)
            if ret is None:  # removed line
                removed_lines += 1
            if ret:
                #print(json.dumps(ret))
                r = json.loads(results[i - removed_lines])
                assert ret == r, Exception(i, line, ret, (i-removed_lines), r)

    assert len(results) == len(lines)-removed_lines, Exception(len(results), len(lines), removed_lines)


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
    assert t( ("data", CSV(r"city, pop, province", quote='|') ))({'data': '|nj|, |800|, |jiang, su|'})  == {'province': 'jiang, su', 'city': 'nj', 'data': '|nj|, |800|, |jiang, su|', 'pop': '800'}

    # restrict
    assert t(("data", CSV(r"city, pop, province")))({'data': 'nj,800,js,gudu'})  == {'province': 'js', 'city': 'nj', 'data': 'nj,800,js,gudu', 'pop': '800'}
    assert t(("data", CSV(r"city, pop, province", restrict=True)))({'data': 'nj,800,js,gudu'})  == {'data': 'nj,800,js,gudu'}
    assert t(("data", CSV(r"city, pop, province", restrict=True)))({'data': 'nj,800'})  == {'data': 'nj,800'}

    # TSV
    assert t( ("data", TSV(r"city,pop,province") ))({'data': 'nj\t800\tjs'})  == {'province': 'js', 'city': 'nj', 'data': 'nj\t800\tjs', 'pop': '800'}


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


def _pre_csv(content):
    file_path = './tmp_test_lookup_csv'
    with open(file_path, "w") as f:
        f.write(content)
    return file_path


def test_lookup_csv():
    # default
    csv = _pre_csv("city,pop,province\nnj,800,js\nsh,2000,sh")
    assert t( ("city", LOOKUP(csv, ["province", "pop"]) ))({'data': '123', 'city': 'nj'})  == {'data': '123', 'city': 'nj', 'province': 'js', 'pop': '800'}

    # file type
    csv = 'file://' + _pre_csv("city,pop,province\nnj,800,js\nsh,2000,sh")
    assert t( ("city", LOOKUP(csv, ["province", "pop"]) ))({'data': '123', 'city': 'nj'})  == {'data': '123', 'city': 'nj', 'province': 'js', 'pop': '800'}

    #
    # assert t(("data", CSV(r"city, pop, province", sep='#')))({'data': 'nj#800#js'}) == {'province': 'js', 'city': 'nj', 'data': 'nj#800#js', 'pop': '800'}
    #
    # # config
    # assert t( ("data", CSV(['city', 'pop', 'province']) ))({'data': 'nj, 800, js'})  == {'province': 'js', 'city': 'nj', 'data': 'nj, 800, js', 'pop': '800'}
    #
    # # lstrip
    # assert t( ("data", CSV(r"city, pop, province") ))({'data': 'nj, 800, js'})  == {'province': 'js', 'city': 'nj', 'data': 'nj, 800, js', 'pop': '800'}
    # assert t( ("data", CSV(r"city, pop, province", lstrip=False) ))({'data': 'nj, 800, js'})  == {'province': ' js', 'city': 'nj', 'data': 'nj, 800, js', 'pop': ' 800'}
    #
    # # quote
    # assert t( ("data", CSV(r"city, pop, province") ))({'data': '"nj", "800", "js"'})  == {'province': 'js', 'city': 'nj', 'data': '"nj", "800", "js"', 'pop': '800'}
    # assert t( ("data", CSV(r"city, pop, province") ))({'data': '"nj", "800", "jiang, su"'})  == {'province': 'jiang, su', 'city': 'nj', 'data': '"nj", "800", "jiang, su"', 'pop': '800'}
    # assert t( ("data", CSV(r"city, pop, province", quote='|') ))({'data': '|nj|, |800|, |jiang, su|'})  == {'province': 'jiang, su', 'city': 'nj', 'data': '|nj|, |800|, |jiang, su|', 'pop': '800'}
    #
    # # restrict
    # assert t(("data", CSV(r"city, pop, province")))({'data': 'nj,800,js,gudu'})  == {'province': 'js', 'city': 'nj', 'data': 'nj,800,js,gudu', 'pop': '800'}
    # assert t(("data", CSV(r"city, pop, province", restrict=True)))({'data': 'nj,800,js,gudu'})  == {'data': 'nj,800,js,gudu'}
    # assert t(("data", CSV(r"city, pop, province", restrict=True)))({'data': 'nj,800'})  == {'data': 'nj,800'}
    #
    # # TSV
    # assert t( ("data", TSV(r"city,pop,province") ))({'data': 'nj\t800\tjs'})  == {'province': 'js', 'city': 'nj', 'data': 'nj\t800\tjs', 'pop': '800'}
    #


test_lookup_csv()
exit(1)

test_condition()
test_regex()
test_csv()
test_lookup_dict()
test_dispatch_transform()
test_meta()
test_parse()
test_runner()
test_module()



