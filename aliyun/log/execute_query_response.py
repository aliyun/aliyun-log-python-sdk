#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

from .logresponse import LogResponse
from .util import Util


class ExecuteQueryResponse(LogResponse):
    """ The response of the execute_query API from log.

    :type header: dict
    :param header: ExecuteQueryResponse HTTP response header

    :type resp: dict
    :param resp: HTTP response body
    """

    def __init__(self, header, resp):
        LogResponse.__init__(self, header, resp)
        self._meta = resp.get('meta', {}) if isinstance(resp, dict) else {}

    def get_meta(self):
        """Get the meta field

        :return: dict, containing affectedRows and elapsedMillisecond
        """
        return self._meta

    def get_affected_rows(self):
        """Get the number of affected rows

        :return: int or None, the number of affected rows if exists
        """
        return self._meta.get('affectedRows')

    def get_elapsed_millisecond(self):
        """Get the elapsed time in milliseconds

        :return: int or None, the elapsed time in milliseconds if exists
        """
        return self._meta.get('elapsedMillisecond')

    def log_print(self):
        """Print response information"""
        print('ExecuteQueryResponse:')
        print('headers:', self.get_all_headers())
        print('meta:', self._meta)
        print('body:', self.body)

