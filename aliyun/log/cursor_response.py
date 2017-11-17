#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

from .util import Util
from .logresponse import LogResponse


class GetCursorResponse(LogResponse):
    """ The response of the get_cursor API from log.
    
    :type header: dict
    :param header: ListShardResponse HTTP response header

    :type resp: dict
    :param resp: the HTTP response body
    """

    def __init__(self, resp, header):
        LogResponse.__init__(self, header, resp)
        self.cursor = Util.convert_unicode_to_str(resp["cursor"])

    def get_cursor(self):
        """

        :return:
        """
        return self.cursor

    def log_print(self):
        print('GetCursorResponse')
        print('headers:', self.get_all_headers())
        print('cursor:', self.cursor)
