from aliyun.log.etl_core import *

# keep events, AND in dict (string, regex)
sls_en_ke1 = keep_event({"__topic__": "ddos_access_log", "cc_phase": r".+"})


# drop condition, OR in outer list  (built-in condition: EMPTY)
sls_en_de1 = drop_event([EMPTY("status"), EMPTY("http_user_agent"),
                           {"http_user_agent": "-"}, {"status": r"2\d+"}])

# keep fields, field list
sls_en_kf1 = KEEP_F(["http_user_agent", "real_client_ip", "remote_addr", "status", "cc_phase"])

# dispath event:
#   condition , dict-string
#   built-in condition (True), built-in function (Drop)
sls_en_dse1 = dispatch_event([
    ({"status": lambda x: int(x) // 100 == 3}, {"error_type": "challenge"}),
    ({"status": lambda x: int(x) // 100 == 4}, {"error_type": "auth"}),
    ({"status": lambda x: int(x) // 100 == 5}, {"error_type": "internal"}),
    (True, DROP)
])

# transform event:
#     built-in condition (EMPTY): dict-built-in function (V)
#     built-in condition (ALL=ANY=True): tuple-field, built-in transform (V)
#     dict condition (field-regex): tuple-field,
sls_en_te1 = transform_event([
    ( EMPTY("real_client_ip"), {"real_client_ip": V('remote_addr')} ),
    ( ANY, ("ip", V('real_client_ip') ) ),
    ( {"http_user_agent": r".+\bWindows\b.+"}, {"is_windows": "1"} ),
    ( {"http_user_agent": r".+\bLinux\b.+"}, {"is_linux": "1"} ),
    ( NO_EMPTY("real_client_ip"), {"real_client_ip":
                                       lambda x: x["real_client_ip"][:3] + "*****" + x["real_client_ip"][-3:] })
])

sls_en_df1 = DROP_F(["remote_addr"])
sls_en_df2 = DROP_F("http_user_agent")

sls_en_rf1 = RENAME({"real_client_ip": "client_ip"})

