#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

from .util import Util
from .logresponse import LogResponse
from .logtail_config_detail import LogtailConfigGenerator

__all__ = ['CreateLogtailConfigResponse', 'DeleteLogtailConfigResponse',
           'GetLogtailConfigResponse', 'UpdateLogtailConfigResponse',
           'ListLogtailConfigResponse']


class CreateLogtailConfigResponse(LogResponse):
    """ The response of the create_logtail_config API from log.

    :type header: dict
    :param header: CreateLogtailConfigResponse HTTP response header
    """

    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('CreateLogtailConfigResponse:')
        print('headers:', self.get_all_headers())


class DeleteLogtailConfigResponse(LogResponse):
    """ The response of the delete_logtail_config API from log.

    :type header: dict
    :param header: DeleteLogtailConfigResponse HTTP response header
    """

    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

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
        LogResponse.__init__(self, header, resp)
        self.logtail_config = LogtailConfigGenerator.generate_config(resp)

    def log_print(self):
        print('GetLogtailConfigResponse:')
        print('headers:', self.get_all_headers())
        print('logtail_config:', self.logtail_config.to_json())


class UpdateLogtailConfigResponse(LogResponse):
    """ The response of the update_logtail_config API from log.

    :type header: dict
    :param header: UpdateLogtailConfigResponse HTTP response header
    """

    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

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
        LogResponse.__init__(self, header, resp)
        self.count = int(resp["count"])
        self.total_count = int(resp["total"])
        self.logtail_configs = Util.convert_unicode_to_str(resp.get("configs", []))

    def get_configs(self):
        return self.logtail_configs

    def get_configs_count(self):
        return self.count

    def get_configs_total(self):
        return self.total_count

    def get_count(self):
        return self.count

    def get_total(self):
        return self.total_count

    @property
    def total(self):
        return self.total_count

    def log_print(self):
        print('ListLogtailConfigResponse:')
        print('headers:', self.get_all_headers())
        print('configs_count:', str(self.count))
        print('configs_total:', str(self.total_count))
        print('configs:', str(self.logtail_configs))

    def merge(self, response):
        if not isinstance(response, ListLogtailConfigResponse):
            raise ValueError("passed response is not a ListLogtailConfigResponse: " + str(type(response)))

        self.count += response.get_configs_count()
        self.total_count = response.get_configs_total() # use the latest total count
        self.logtail_configs.extend(response.get_configs())

        # update body
        self.body = {
            'count': self.count,
            'total': self.total_count,
            'configs': self.logtail_configs
        }

        return self
