#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

from .logresponse import LogResponse
from .async_sql_pb2 import AsyncSqlResponsePB
from .compress import Compressor


def _get_or_default(pb, field, default):
    if pb is None:
        return default
    return pb.HasField(field) and getattr(pb, field) or default

class AsyncSqlResponse(LogResponse):
    """ The response used for async SQL operations.
    
    :type header: dict
    :param header: response header

    :type resp: string
    :param resp: response data
    """

    def __init__(self, header, resp):
        LogResponse.__init__(self, header, resp)
        self.async_sql_response_pb = AsyncSqlResponsePB()
        if resp:
            try:
                raw_data = Compressor.decompress_response(header, resp)
                self.async_sql_response_pb.ParseFromString(raw_data)
            except Exception as ex:
                raise Exception("Failed to parse AsyncSqlResponsePB: {0}".format(ex))

    def get_query_id(self):
        """ Get async SQL query ID

        :return: string, query ID
        """
        return self.async_sql_response_pb.id

    def get_state(self):
        """ Get async SQL query state

        :return: string, query state (e.g., "RUNNING", "COMPLETE", "FAILED")
        """
        return self.async_sql_response_pb.state

    def get_error_code(self):
        """ Get error code if query failed

        :return: string, error code
        """
        return _get_or_default(self.async_sql_response_pb, 'error_code', None)

    def get_error_message(self):
        """ Get error message if query failed

        :return: string, error message
        """
        return _get_or_default(self.async_sql_response_pb, 'error_message', None)

    def get_meta(self):
        """ Get query metadata

        :return: AsyncSqlMetaPB, query metadata
        """
        return _get_or_default(self.async_sql_response_pb, 'meta', None)

    def get_result_rows(self):
        """ Get number of result rows

        :return: int, number of result rows
        """
        meta = self.get_meta()
        return _get_or_default(meta, 'result_rows', 0)

    def get_processed_rows(self):
        """ Get number of processed rows

        :return: int, number of processed rows
        """
        meta = self.get_meta()
        return _get_or_default(meta, 'processed_rows', 0)

    def get_processed_bytes(self):
        """ Get number of processed bytes

        :return: int, number of processed bytes
        """
        meta = self.get_meta()
        return _get_or_default(meta, 'processed_bytes', 0)

    def get_elapsed_milli(self):
        """ Get elapsed time in milliseconds

        :return: int, elapsed time in milliseconds
        """
        meta = self.get_meta()
        return _get_or_default(meta, 'elapsed_milli', 0)

    def get_cpu_sec(self):
        """ Get CPU time in seconds

        :return: float, CPU time in seconds
        """
        meta = self.get_meta()
        return _get_or_default(meta, 'cpu_sec', 0.0)

    def get_cpu_cores(self):
        """ Get CPU cores used

        :return: int, CPU cores used
        """
        meta = self.get_meta()
        return _get_or_default(meta, 'cpu_cores', 0)

    def get_progress(self):
        """ Get query progress

        :return: string, query progress
        """
        meta = self.get_meta()
        return _get_or_default(meta, 'progress', '')

    def get_keys(self):
        """ Get column names/keys

        :return: list, column names
        """
        meta = self.get_meta()
        return list(meta.keys) if meta else []

    def get_rows(self):
        """ Get result rows

        :return: list, result rows data
        """
        rows = []
        for row_pb in self.async_sql_response_pb.rows:
            rows.append(list(row_pb.columns))
        return rows

    def get_raw_response_pb(self):
        """ Get raw protobuf response

        :return: AsyncSqlResponsePB, raw protobuf response
        """
        return self.async_sql_response_pb

    def log_print(self):
        """ Print response information for debugging
        """
        print("AsyncSqlResponse:")
        print("  Query ID: {0}".format(self.get_query_id()))
        print("  State: {0}".format(self.get_state()))
        print("  Result Rows: {0}".format(self.get_result_rows()))
        print("  Processed Rows: {0}".format(self.get_processed_rows()))
        print("  Processed Bytes: {0}".format(self.get_processed_bytes()))
        print("  Elapsed Time: {0}ms".format(self.get_elapsed_milli()))
        print("  CPU Time: {0}s".format(self.get_cpu_sec()))
        print("  CPU Cores: {0}".format(self.get_cpu_cores()))
        print("  Progress: {0}".format(self.get_progress()))
        if self.get_error_code():
            print("  Error Code: {0}".format(self.get_error_code()))
            print("  Error Message: {0}".format(self.get_error_message()))
        print("  Column Names: {0}".format(self.get_keys()))
        print("  Data Rows: {0}".format(len(self.get_rows())))
