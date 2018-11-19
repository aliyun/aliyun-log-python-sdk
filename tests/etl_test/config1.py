from aliyun.log.etl_core import *


KEEP_EVENT_default = True

KEEP_EVENT_pass_or_failure = [{"result": r"(?i)ok|pass"}, {"status": lambda v: int(v) == 200},
                                    lambda e: (('status' in e and int(e['status']) > 200) or ('__raw__' in e and 'error' in e['__raw__']))]

DROP_EVENT_useless = {"data": "useless code.+|useless2 code.+", "raw": "bad data.+"}

KEEP_FIELDS_all = '.+'

DROP_FIELDS_none = ""

RENAME_FIELDS_simple = {"f1": "f1_new", "f2": "f2_new"}


def sls_eu_my_logic(event):
    event["hello"] = "world"
    return event


DISPATCH_EVENT_data = [
    ({"data": "^ETL_Information .+"}, {"__topic__": "etl_info"}),
    ({"data": "^Status .+"}, {"__topic__": "machine_status"}),
    ({"data": "^System Reboot .+"}, {"__topic__": "reboot_event"}),
    ({"data": "^Provision Firmware Download start .+"}, {"__topic__": "download"}),
    (True, {"__topic__": "unknown"})]

@condition({"__topic__": "etl_info"})
def sls_eu_parse_data(event):
    return event

TRANSFORM_EVENT_data = [
    ({"__topic__": "etl_info"}, {"__topic__": "etl_info"}),
    ({"__topic__": "machine_status"}, {"__topic__": "machine_status"}),
    ({"__topic__": "reboot_event"}, {"__topic__": "reboot_event"}),
    ({"__topic__": "download"}, {"__topic__": "download"}),
    (True, {"__topic__": "unknown"})]

