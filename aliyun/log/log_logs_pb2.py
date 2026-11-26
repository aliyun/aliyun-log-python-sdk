"""Protobuf bindings selected for the installed runtime."""

from ._protobuf import load as _load

_pb2 = _load('log_logs_pb2')
DESCRIPTOR = _pb2.DESCRIPTOR
Log = _pb2.Log
LogTag = _pb2.LogTag
LogGroup = _pb2.LogGroup
LogGroupList = _pb2.LogGroupList
