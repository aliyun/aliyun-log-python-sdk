import sys

if sys.version_info[0] == 2:
    from .py2_legacy.log_logs_raw_pb2 import LogGroupListRaw, LogGroupRaw, LogTagRaw, LogRaw
    from .py2_legacy.log_logs_pb2 import LogGroupList, LogGroup, LogTag, Log
else:
    from .log_logs_raw_pb2 import LogGroupListRaw, LogGroupRaw, LogTagRaw, LogRaw
    from .log_logs_pb2 import LogGroupList, LogGroup, LogTag, Log

__all__ = [
    'LogGroupListRaw',
    'LogGroupRaw',
    'LogTagRaw',
    'LogRaw',
    'LogGroupList',
    'LogGroup',
    'LogTag',
    'Log'
]