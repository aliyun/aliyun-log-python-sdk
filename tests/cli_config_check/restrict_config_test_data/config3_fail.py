from aliyun.log.etl_core import *

# keep events, AND in dict (string, regex)
KEEP_EVENT_a = {"__topic__": "ddos_access_log", "cc_phase": r".+"}


# drop condition, OR in outer list  (built-in condition: EMPTY)
DROP_EVENT_b = [EMPTY("status"), EMPTY("http_user_agent"),
                           {"http_user_agent": "-"}, {"status": r"2\d+"}]

# keep fields, field list
KEEP_FIELDS_a = ["http_user_agent", "real_client_ip", "remote_addr", "status", "cc_phase"]


# transform event:
#     built-in condition (EMPTY): dict-built-in function (V)
#     built-in condition (ALL=ANY=True): tuple-field, built-in transform (V)
#     dict condition (field-regex): tuple-field,
TRANSFORM_EVENT_v = [
    ( EMPTY("real_client_ip"), {"real_client_ip": V('remote_addr')} ),
    ( ANY2, ("ip", V('real_client_ip') ) ),
    ( {"http_user_agent": r".+\bWindows\b.+"}, {"is_windows": "1"} ),
    ( {"http_user_agent": r".+\bLinux\b.+"}, {"is_linux": "1"} ),
    ( NO_EMPTY("real_client_ip"), {"real_client_ip": "xyz" } ) ]

