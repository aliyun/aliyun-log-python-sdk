"""Protobuf bindings selected for the installed runtime."""

from ._protobuf import load as _load

_pb2 = _load('async_sql_pb2')
DESCRIPTOR = _pb2.DESCRIPTOR
AsyncSqlMetaPB = _pb2.AsyncSqlMetaPB
AsyncSqlRowPB = _pb2.AsyncSqlRowPB
AsyncSqlResponsePB = _pb2.AsyncSqlResponsePB
