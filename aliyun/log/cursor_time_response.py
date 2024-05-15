#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

from .logresponse import LogResponse


class GetCursorTimeResponse(LogResponse):
    """ The response of the get_cursor_time API from log.

    :type header: dict
    :param header: GetCursorTimeResponse HTTP response header

    :type resp: dict
    :param resp: the HTTP response body
    """

    def __init__(self, resp, header):
        LogResponse.__init__(self, header, resp)
        self.cursor_time = resp['cursor_time']

    def get_cursor_time(self):
        """

        :return:
        """
        return self.cursor_time

    def log_print(self):
        print('GetCursorTimeResponse')
        print('headers:', self.get_all_headers())
        print('cursor_time:', self.cursor_time)
