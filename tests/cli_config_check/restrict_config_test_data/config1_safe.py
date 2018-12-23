from aliyun.log.etl_core import *


KEEP_EVENT_default = True


DROP_EVENT_useless = {"data": "useless code.+|useless2 code.+", "raw": "bad data.+"}

KEEP_FIELDS_all = '.+'

DROP_FIELDS_none = ""

RENAME_FIELDS_simple = {"f1": "f1_new", "f2": "f2_new"}


DISPATCH_EVENT_data = [
    ({"data": "^ETL_Information .+"}, {"__topic__": "etl_info"}),
    ({"data": "^Status .+"}, {"__topic__": "machine_status"}),
    ({"data": "^System Reboot .+"}, {"__topic__": "reboot_event"}),
    ({"data": "^Provision Firmware Download start .+"}, {"__topic__": "download"}),
    (True, {"__topic__": "unknown"})]


TRANSFORM_EVENT_data = [
    ({"__topic__": "etl_info"}, {"__topic__": "etl_info"}),
    ({"__topic__": "machine_status"}, {"__topic__": "machine_status"}),
    ({"__topic__": "reboot_event"}, {"__topic__": "reboot_event"}),
    ({"__topic__": "download"}, {"__topic__": "download"}),
    (True, {"__topic__": "unknown"})]

