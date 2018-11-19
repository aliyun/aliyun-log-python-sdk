from aliyun.log.etl_core import *

# keep events, AND in dict (string, regex)
KEEP_EVENT_access_log = {"__topic__": "ddos_access_log", "cc_phase": r".+"}

# drop condition, OR in outer list  (built-in condition: EMPTY)
DROP_EVENT_200_no_agent = [EMPTY("status"), EMPTY("http_user_agent"),
                           {"http_user_agent": "-"}, {"status": r"2\d+"}]

# keep fields, field list
KEEP_FIELDS_user = ["http_user_agent", "real_client_ip", "remote_addr", "status", "cc_phase"]

# dispath event:
#   condition , dict-string
#   built-in condition (True), built-in function (Drop)
DISPATCH_EVENT_status = [
    ({"status": lambda x: int(x) // 100 == 3}, {"error_type": "challenge"}),
    ({"status": lambda x: int(x) // 100 == 4}, {"error_type": "auth"}),
    ({"status": lambda x: int(x) // 100 == 5}, {"error_type": "internal"}),
    (True, DROP)
]

# transform event:
#     built-in condition (EMPTY): dict-built-in function (V)
#     built-in condition (ALL=ANY=True): tuple-field, built-in transform (V)
#     dict condition (field-regex): tuple-field,
TRANSFORM_EVENT_ip = [
    ( EMPTY("real_client_ip"), {"real_client_ip": V('remote_addr')} ),
    ( ANY, ("ip", V('real_client_ip') ) ),
    ( {"http_user_agent": r".+\bWindows\b.+"}, {"is_windows": "1"} ),
    ( {"http_user_agent": r".+\bLinux\b.+"}, {"is_linux": "1"} ),
    ( NO_EMPTY("real_client_ip"), {"real_client_ip":
                                       lambda x: x["real_client_ip"][:3] + "*****" + x["real_client_ip"][-3:] })
]

DROP_FIELDS_remote = ["remote_addr", "http_user_agent"]
ALIAS_ip = {"real_client_ip": "client_ip"}

