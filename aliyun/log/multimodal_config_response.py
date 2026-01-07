#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

from .util import Util
from .logresponse import LogResponse


class GetLogStoreMultimodalConfigurationResponse(LogResponse):
    """ The response of the get_logstore_multimodal_configuration API from log.

    :type header: dict
    :param header: GetLogStoreMultimodalConfigurationResponse HTTP response header

    :type resp: dict
    :param resp: the HTTP response body
    """

    def __init__(self, resp, header):
        LogResponse.__init__(self, header, resp)
        self.status = Util.convert_unicode_to_str(resp["status"])
        self.anonymous_write = None
        if "anonymousWrite" in resp:
            self.anonymous_write = Util.convert_unicode_to_str(resp["anonymousWrite"])

    def get_status(self):
        """
        :return: string, the status of the multimodal configuration
        """
        return self.status

    def get_anonymous_write(self):
        """
        :return: string, the anonymous write setting, or None if not set
        """
        return self.anonymous_write

    def log_print(self):
        print('GetLogStoreMultimodalConfigurationResponse')
        print('headers:', self.get_all_headers())
        print('status:', self.status)
        if self.anonymous_write is not None:
            print('anonymousWrite:', self.anonymous_write)


class PutLogStoreMultimodalConfigurationResponse(LogResponse):
    """ The response of the put_logstore_multimodal_configuration API from log.

    :type header: dict
    :param header: PutLogStoreMultimodalConfigurationResponse HTTP response header
    """

    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('PutLogStoreMultimodalConfigurationResponse:')
        print('headers:', self.get_all_headers())

