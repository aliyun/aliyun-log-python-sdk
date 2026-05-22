

TRANSFORM_EVENT_topic = True, {"__topic__": "saf_access_log"}

KEEP_EVENT_t1 = {"instance_id": "account_abuse_pro|coupon_abuse_pro|account_takeover_pro"}


RENAME_FIELDS_t1 = {"userId": "user_id", "requestParams": "request_param",
                    "deviceInfo": "device_info"}

KEEP_FIELDS_test1 = ["__topic__", "__time__",
                     "product_code", "instance_id", "timestamp", "req_id",
                     "uid", "user_id", "region", "score",
                     "request_param", "device_info", "tags", "code"]


from aliyun.log.etl_core import *
#import re

KEEP_FIELDS_f1 = [F_TIME, "acl_action", "acl_blocks",
                  "aliwaf_action", "aliwaf_rule_type", "body_bytes_sent",
                  "content_type", "eagleeye_traceid", "host$", "http_cookie",
                  "http_referer", "http_user_agent", "http_x_forwarded_for",
                  "https", "matched_host", "real_client_ip", "region",
                  "remote_addr", "remote_port", "request_length", "request_method",
                  "request_time_msec", "server_name", "server_protocol",
                  "status", "time", "tmd_action", "tmd_blocks", "tmd_phase",
                  r"ua_\w+", "upstream_addr", "upstream_ip", "upstream_response_time",
                  "upstream_status", "user_id", "block_action", "eagleeye_traceid",
                  "antibot", "antibot_action", "request_path", "querystring"]

RENAME_FIELDS_r1 = {"aliwaf_action": "waf_action",
                    "aliwaf_rule_type": "web_attack_type",
                    "tmd_action": "cc_action",
                    "tmd_blocks": "cc_blocks",
                    "tmd_phase": "cc_phase",
                    "eagleeye_traceid": "request_traceid"
                    }

TRANSFORM_EVENT_t1 = (ANY, {'__topic__': 'waf_access_log', '__source__': "log_service"})
