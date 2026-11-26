"""Protobuf bindings selected for the installed runtime."""

from ._protobuf import load as _load

_pb2 = _load('log_logs_raw_pb2')
DESCRIPTOR = _pb2.DESCRIPTOR
LogRaw = _pb2.LogRaw
LogTagRaw = _pb2.LogTagRaw
LogGroupRaw = _pb2.LogGroupRaw
LogGroupListRaw = _pb2.LogGroupListRaw
