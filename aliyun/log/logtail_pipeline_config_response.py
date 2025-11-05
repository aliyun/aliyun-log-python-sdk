#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

"""
Logtail Pipeline Config Response Classes

This module provides response classes for Logtail pipeline config API operations.
"""

from .logresponse import LogResponse
from .util import Util
from .logtail_pipeline_config_detail import LogtailPipelineConfigDetail

__all__ = ['CreateLogtailPipelineConfigResponse', 'DeleteLogtailPipelineConfigResponse',
           'GetLogtailPipelineConfigResponse', 'UpdateLogtailPipelineConfigResponse',
           'ListLogtailPipelineConfigResponse']


class CreateLogtailPipelineConfigResponse(LogResponse):
    """The response of the create_logtail_pipeline_config API from log.

    :type header: dict
    :param header: CreateLogtailPipelineConfigResponse HTTP response header
    """

    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('CreateLogtailPipelineConfigResponse:')
        print('headers:', self.get_all_headers())


class DeleteLogtailPipelineConfigResponse(LogResponse):
    """The response of the delete_logtail_pipeline_config API from log.

    :type header: dict
    :param header: DeleteLogtailPipelineConfigResponse HTTP response header
    """

    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('DeleteLogtailPipelineConfigResponse:')
        print('headers:', self.get_all_headers())


class GetLogtailPipelineConfigResponse(LogResponse):
    """The response of the get_logtail_pipeline_config API from log.

    :type header: dict
    :param header: GetLogtailPipelineConfigResponse HTTP response header

    :type resp: dict
    :param resp: the HTTP response body
    """

    def __init__(self, resp, header):
        LogResponse.__init__(self, header, resp)
        self.pipeline_config = LogtailPipelineConfigDetail.from_json(resp)

    def get_pipeline_config(self):
        """Get the pipeline config detail

        :return: LogtailPipelineConfigDetail object
        """
        return self.pipeline_config

    def log_print(self):
        print('GetLogtailPipelineConfigResponse:')
        print('headers:', self.get_all_headers())
        print('pipeline_config:', self.pipeline_config.to_json())


class UpdateLogtailPipelineConfigResponse(LogResponse):
    """The response of the update_logtail_pipeline_config API from log.

    :type header: dict
    :param header: UpdateLogtailPipelineConfigResponse HTTP response header
    """

    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('UpdateLogtailPipelineConfigResponse:')
        print('headers:', self.get_all_headers())


class ListLogtailPipelineConfigResponse(LogResponse):
    """The response of the list_logtail_pipeline_config API from log.

    :type header: dict
    :param header: ListLogtailPipelineConfigResponse HTTP response header

    :type resp: dict
    :param resp: the HTTP response body
    """

    def __init__(self, resp, header):
        LogResponse.__init__(self, header, resp)
        self.count = int(resp.get("count", 0))
        self.total = int(resp.get("total", 0))
        self.configs = Util.convert_unicode_to_str(resp.get("configs", []))

    def get_configs(self):
        """Get the list of pipeline config names

        :return: list of config names
        """
        return self.configs

    def get_count(self):
        """Get the count of configs in current response

        :return: int
        """
        return self.count

    def get_total(self):
        """Get the total count of configs

        :return: int
        """
        return self.total

    def log_print(self):
        print('ListLogtailPipelineConfigResponse:')
        print('headers:', self.get_all_headers())
        print('count:', str(self.count))
        print('total:', str(self.total))
        print('configs:', str(self.configs))

    def merge(self, response):
        """Merge another response into current response

        :type response: ListLogtailPipelineConfigResponse
        :param response: another response to merge

        :return: self
        """
        if not isinstance(response, ListLogtailPipelineConfigResponse):
            raise ValueError(
                "passed response is not a ListLogtailPipelineConfigResponse: " + str(type(response)))

        self.count += response.get_count()
        self.total = response.get_total()  # use the latest total count
        self.configs.extend(response.get_configs())

        # update body
        self.body = {
            'count': self.count,
            'total': self.total,
            'configs': self.configs
        }

        return self
