#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

from .logresponse import LogResponse
from .util import Util

__all__ = ['PutObjectResponse', 'GetObjectResponse']


class PutObjectResponse(LogResponse):
    """ The response of the put_object API from log.

    :type header: dict
    :param header: PutObjectResponse HTTP response header

    :type resp: string
    :param resp: PutObjectResponse HTTP response body
    """

    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def get_etag(self):
        """ Get the ETag of the uploaded object.

        :return: string, ETag value
        """
        return Util.h_v_td(self.headers, 'ETag', None)

    def log_print(self):
        print('PutObjectResponse:')
        print('headers:', self.get_all_headers())


class GetObjectResponse(LogResponse):
    """ The response of the get_object API from log.

    :type header: dict
    :param header: GetObjectResponse HTTP response header

    :type resp: bytes
    :param resp: GetObjectResponse HTTP response body (object content)
    """

    def __init__(self, header, resp):
        LogResponse.__init__(self, header, resp)

    def get_etag(self):
        """ Get the ETag of the object.

        :return: string, ETag value, may be None if not set
        """
        return Util.h_v_td(self.headers, 'ETag', None)

    def get_content_type(self):
        """ Get the content type of the object.

        :return: string, content type
        """
        return Util.h_v_td(self.headers, 'Content-Type', '')

    def get_headers(self):
        """ Get all headers of the response.

        :return: dict, all response headers
        """
        return self.headers

    def get_body(self):
        """ Get the object content.

        :return: bytes, object content
        """
        return self.body

    def log_print(self):
        print('GetObjectResponse:')
        print('headers:', self.get_all_headers())

