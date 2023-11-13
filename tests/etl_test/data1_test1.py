
# drop fields - regex
DROP_FIELDS_meta = "__.+__"

# dispatch
#      dict-regex/bool : dict-string
DISPATCH_EVENT_data = [
    ({"data": "^LTE_Information .+"}, {"__topic__": "let_info"}),
    ({"data": "^Status,.+"}, {"__topic__": "machine_status"}),
    ({"data": "^System Reboot .+"}, {"__topic__": "reboot_event"}),
    ({"data": "^Provision Firmware Download start .+"}, {"__topic__": "download"}),
    (True, {"__topic__": "default"})]

# transform
#      dict-regex : regex-tuple
TRANSFORM_EVENT_data = [
    ({"__topic__": "let_info"}, ("data", r"LTE_Information (?P<RSPR>[^,]+),(?P<SINR>[^,]+),(?P<global_cell_id>[^,]+),")),
    ({"__topic__": "machine_status"}, ("data", r"Status,(?P<cpu_usage_usr>[\d\.]+)% usr (?P<cpu_usage_sys>[\d\.]+)% sys,(?P<memory_free>\d+)(?P<memory_free_unit>[a-zA-Z]+),")),
    ({"__topic__": "reboot_event"}, ("data", r"System Reboot \[(?P<reboot_code>\d+)\]")),
    ({"__topic__": "download"}, ("data", r"Provision Firmware Download start \[(?P<firmware_version>[\w\.]+)\]")),
    ]
