#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

__all__ = ['CreateSqlInstanceResponse', 'UpdateSqlInstanceResponse', 'ListSqlInstanceResponse']

from .logresponse import LogResponse


class CreateSqlInstanceResponse(LogResponse):
    """ The response of the create_sql_instance API from log.

    :type header: dict
    :param header: CreateSqlInstanceResponse HTTP response header
    """

    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('CreateSqlInstanceResponse:')
        print('request_id:', self.get_request_id())
        print('headers:', self.get_all_headers())
        print('response:', self.get_body())


class UpdateSqlInstanceResponse(LogResponse):
    """ The response of the update_sql_instance API from log.

    :type header: dict
    :param header: UpdateSqlInstanceResponse HTTP response header
    """

    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('UpdateSqlInstanceResponse:')
        print('request_id:', self.get_request_id())
        print('headers:', self.get_all_headers())
        print('response:', self.get_body())


class ListSqlInstanceResponse(LogResponse):
    """ The response of the list_sql_instance API from log.

    :type header: dict
    :param header: ListSqlInstanceResponse HTTP response header
    """

    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('ListSqlInstanceResponse:')
        print('request_id:', self.get_request_id())
        print('headers:', self.get_all_headers())
        print('response:', self.get_body())
