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
        return Util.h_v_td(self.headers, 'etag', '')

    def log_print(self):
        print('PutObjectResponse:')
        print('headers:', self.get_all_headers())
        print('etag:', self.get_etag())


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

        :return: string, ETag value
        """
        return Util.h_v_td(self.headers, 'etag', '')

    def get_last_modified(self):
        """ Get the last modified time of the object.

        :return: string, last modified time
        """
        return Util.h_v_td(self.headers, 'last-modified', '')

    def get_content_type(self):
        """ Get the content type of the object.

        :return: string, content type
        """
        return Util.h_v_td(self.headers, 'content-type', '')

    def get_headers(self):
        """ Get all headers of the response.

        :return: dict, all response headers
        """
        return self.headers

    def get_body_length(self):
        """ Get the length of the object content.

        :return: int, content length
        """
        content_length = Util.h_v_td(self.headers, 'content-length', '0')
        try:
            return int(content_length)
        except (ValueError, TypeError):
            return 0

    def get_body(self):
        """ Get the object content.

        :return: bytes, object content
        """
        return self.body

    def log_print(self):
        print('GetObjectResponse:')
        print('headers:', self.get_all_headers())
        print('etag:', self.get_etag())
        print('last_modified:', self.get_last_modified())
        print('content_type:', self.get_content_type())
        print('body_length:', self.get_body_length())

