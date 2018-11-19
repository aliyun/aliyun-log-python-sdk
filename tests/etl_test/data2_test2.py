from aliyun.log.etl_core import *

# keep events, AND in dict (string, regex)
KEEP_EVENT_access_log = {"__topic__": "ddos_access_log", "cc_phase": r".+"}

# drop condition, OR in outer list  (built-in condition: EMPTY)
DROP_EVENT_200_no_agent = [EMPTY("status"), EMPTY("http_user_agent"),
                           {"http_user_agent": "-"}, {"status": r"2\d+"}]

# keep fields, field list
KEEP_FIELDS_user = ["http_user_agent", "real_client_ip", "remote_addr", "status", "cc_phase"]


def sls_en_dispatch(event):
    if "status" in event and event["status"].isdigit():
        r = int(event['status']) // 100
        if r == 3:
            event['error_type'] = 'challenge'
            return event
        elif r == 4:
            event['error_type'] = 'auth'
            return event
        elif r == 5:
            event['error_type'] = 'internal'
            return event

    # drop by default
    return None


# transform event:
#     built-in condition (EMPTY): dict-built-in function (V)
#     built-in condition (ALL=ANY=True): tuple-field, built-in transform (V)
#     dict condition (field-regex): tuple-field,
TRANSFORM_EVENT_ip = [
    ( EMPTY("real_client_ip"), {"real_client_ip": V('remote_addr')} ),
    ( ANY, ("ip", V('real_client_ip') ) ),
]

@condition({"http_user_agent": r".+\bWindows\b.+"})
def sls_eu_windows(event):
    return {"is_windows": "1"}

@condition({"http_user_agent": r".+\bLinux\b.+"})
def sls_en_windows(event):
    event["is_linux"] = "1"
    return event

@condition(NO_EMPTY("real_client_ip"))
def sls_eu_anoymouse_ip(event):
    return {"real_client_ip":
                event["real_client_ip"][:3] + "*****" + event["real_client_ip"][-3:]}

DROP_FIELDS_remote = ["remote_addr", "http_user_agent"]
ALIAS_ip = {"real_client_ip": "client_ip"}

