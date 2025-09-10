#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

from .logresponse import LogResponse
from .async_sql_pb2 import AsyncSqlResponsePB
from .compress import Compressor


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
        return self.async_sql_response_pb.error_code if self.async_sql_response_pb.HasField('error_code') else None

    def get_error_message(self):
        """ Get error message if query failed

        :return: string, error message
        """
        return self.async_sql_response_pb.error_message if self.async_sql_response_pb.HasField('error_message') else None

    def get_meta(self):
        """ Get query metadata

        :return: AsyncSqlMetaPB, query metadata
        """
        return self.async_sql_response_pb.meta if self.async_sql_response_pb.HasField('meta') else None

    def get_result_rows(self):
        """ Get number of result rows

        :return: int, number of result rows
        """
        meta = self.get_meta()
        return meta.result_rows if meta and meta.HasField('result_rows') else 0

    def get_processed_rows(self):
        """ Get number of processed rows

        :return: int, number of processed rows
        """
        meta = self.get_meta()
        return meta.processed_rows if meta and meta.HasField('processed_rows') else 0

    def get_processed_bytes(self):
        """ Get number of processed bytes

        :return: int, number of processed bytes
        """
        meta = self.get_meta()
        return meta.processed_bytes if meta and meta.HasField('processed_bytes') else 0

    def get_elapsed_milli(self):
        """ Get elapsed time in milliseconds

        :return: int, elapsed time in milliseconds
        """
        meta = self.get_meta()
        return meta.elapsed_milli if meta and meta.HasField('elapsed_milli') else 0

    def get_cpu_sec(self):
        """ Get CPU time in seconds

        :return: float, CPU time in seconds
        """
        meta = self.get_meta()
        return meta.cpu_sec if meta and meta.HasField('cpu_sec') else 0.0

    def get_cpu_cores(self):
        """ Get CPU cores used

        :return: int, CPU cores used
        """
        meta = self.get_meta()
        return meta.cpu_cores if meta and meta.HasField('cpu_cores') else 0

    def get_progress(self):
        """ Get query progress

        :return: string, query progress
        """
        meta = self.get_meta()
        return meta.progress if meta and meta.HasField('progress') else ''

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
