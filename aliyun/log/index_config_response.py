#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

from .index_config import *
from .logresponse import LogResponse

__all__ = ['CreateIndexResponse', 'UpdateIndexResponse', 'DeleteIndexResponse', 'GetIndexResponse']


class CreateIndexResponse(LogResponse):
    """ The response of the create_index API from log.

    :type header: dict
    :param header: CreateIndexResponse HTTP response header
    """

    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('CreateIndexResponse:')
        print('headers:', self.get_all_headers())


class UpdateIndexResponse(LogResponse):
    """ The response of the update_index API from log.

    :type header: dict
    :param header: UpdateIndexResponse HTTP response header
    """

    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('UpdateIndexResponse:')
        print('headers:', self.get_all_headers())


class DeleteIndexResponse(LogResponse):
    """ The response of the delete_index API from log.

    :type header: dict
    :param header: DeleteIndexResponse HTTP response header
    """

    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('DeleteIndexResponse:')
        print('headers:', self.get_all_headers())


class GetIndexResponse(LogResponse):
    """ The response of the get_index_config API from log.

    :type header: dict
    :param header: GetIndexResponse HTTP response header

    :type resp: dict
    :param resp: the HTTP response body
    """

    def __init__(self, resp, header):
        LogResponse.__init__(self, header, resp)
        self.index_config = IndexConfig()
        self.index_config.from_json(resp)

    def get_index_config(self):
        """

        :return:
        """
        return self.index_config

    def log_print(self):
        """

        :return:
        """
        print('GetLogStoreResponse:')
        print('headers:', self.get_all_headers())
        print('index_configs:', str(self.index_config.to_json()))
