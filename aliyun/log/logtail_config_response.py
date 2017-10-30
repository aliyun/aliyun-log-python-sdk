#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

__all__ = ['CreateLogtailConfigResponse', 'DeleteLogtailConfigResponse',
           'GetLogtailConfigResponse', 'UpdateLogtailConfigResponse',
           'ListLogtailConfigResponse']

from aliyun.log.util import Util
from aliyun.log.logresponse import LogResponse
from aliyun.log.logtail_config_detail import LogtailConfigHelper


class CreateLogtailConfigResponse(LogResponse):
    """ The response of the create_logtail_config API from log.
    
    :type header: dict
    :param header: CreateLogtailConfigResponse HTTP response header
    """

    def __init__(self, header):
        LogResponse.__init__(self, header)

    def log_print(self):
        print('CreateLogtailConfigResponse:')
        print('headers:', self.get_all_headers())


class DeleteLogtailConfigResponse(LogResponse):
    """ The response of the delete_logtail_config API from log.
    
    :type header: dict
    :param header: DeleteLogtailConfigResponse HTTP response header
    """

    def __init__(self, header):
        LogResponse.__init__(self, header)

    def log_print(self):
        print('DeleteLogtailConfigResponse:')
        print('headers:', self.get_all_headers())


class GetLogtailConfigResponse(LogResponse):
    """ The response of the get_logtail_config API from log.
    
    :type header: dict
    :param header: GetLogtailConfigResponse HTTP response header

    :type resp: dict
    :param resp: the HTTP response body
    """

    def __init__(self, resp, header):
        LogResponse.__init__(self, header)
        self.logtail_config = LogtailConfigHelper.generate_logtail_config(resp)

    def log_print(self):
        print('GetLogtailConfigResponse:')
        print('headers:', self.get_all_headers())
        print('logtail_config:', self.logtail_config.to_json())


class UpdateLogtailConfigResponse(LogResponse):
    """ The response of the update_logtail_config API from log.
    
    :type header: dict
    :param header: UpdateLogtailConfigResponse HTTP response header
    """

    def __init__(self, header):
        LogResponse.__init__(self, header)

    def log_print(self):
        print('UpdateLogtailConfigResponse:')
        print('headers:', self.get_all_headers())


class ListLogtailConfigResponse(LogResponse):
    """ The response of the list_logtail_config API from log.
    
    :type header: dict
    :param header: ListLogtailConfigResponse HTTP response header

    :type resp: dict
    :param resp: the HTTP response body
    """

    def __init__(self, resp, header):
        LogResponse.__init__(self, header)
        self.count = int(resp["count"])
        self.count = int(resp["total"])
        self.logtail_configs = Util.convert_unicode_to_str(resp["configs"])

    def get_configs(self):
        return self.logtail_configs

    def get_configs_count(self):
        return self.count

    def log_print(self):
        print('ListLogtailConfigResponse:')
        print('headers:', self.get_all_headers())
        print('configs_count:', str(self.count))
        print('configs:', str(self.logtail_configs))
