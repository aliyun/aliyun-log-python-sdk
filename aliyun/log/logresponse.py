#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

from .util import Util


class LogResponse(object):
    """ The base response class of all log response.

    :type headers: dict
    :param headers: HTTP response header
    """

    def __init__(self, headers, body=''):
        self.headers = headers
        if body is None:
            body = ''
        self.body = body

    def get_request_id(self):
        """ Get the request id of the response.  '' will be return if not set.

        :return: string, request id
        """
        return Util.h_v_td(self.headers, 'x-log-requestid', '')

    def get_body(self):
        """ Get body

        :return: string
        """
        return self.body

    def get_all_headers(self):
        """ Get all http header of the response

        :return: dict, response header
        """
        return self.headers

    def get_header(self, key):
        """ Get specified http header of the response, '' will be return if not set.

        :type key: string
        :param key: the key to get header

        :return: string, response header
        """
        return self.headers[key] if key in self.headers else ''

    def log_print(self):
        print('header: ', self.headers)
