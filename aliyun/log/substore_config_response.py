#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

__all__ = ['CreateSubStoreResponse', 'DeleteSubStoreResponse', 'GetSubStoreResponse',
           'UpdateSubStoreResponse', 'ListSubStoreResponse', 'GetSubStoreTTLResponse', 'UpdateSubStoreTTLResponse',
           'CreateMetricsStoreResponse']

from .util import Util
from .logresponse import LogResponse


class CreateSubStoreResponse(LogResponse):
    """ The response of the create_substore API from log.

    :type header: dict
    :param header: CreateSubStoreResponse HTTP response header
    """

    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('CreateSubStoreResponse:')
        print('headers:', self.get_all_headers())


class DeleteSubStoreResponse(LogResponse):
    """ The response of the delete_substore API from log.

    :type header: dict
    :param header: DeleteSubStoreResponse HTTP response header
    """

    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('DeleteSubStoreResponse:')
        print('headers:', self.get_all_headers())


class GetSubStoreResponse(LogResponse):
    """ The response of the get_substore API from log.

    :type header: dict
    :param header: GetSubStoreResponse HTTP response header

    :type resp: dict
    :param resp: the HTTP response body
    """

    def __init__(self, resp, header):
        LogResponse.__init__(self, header, resp)
        self.substore_name = Util.convert_unicode_to_str(resp["name"])
        self.ttl = int(resp["ttl"])
        self.sorted_key_count = int(resp["sortedKeyCount"])
        self.time_index = int(resp["timeIndex"])
        self.keys = resp["keys"]

    def get_substore_name(self):
        """

        :return:
        """
        return self.substore_name

    def get_ttl(self):
        """

        :return:
        """
        return self.ttl

    def get_sorted_key_count(self):
        """

        :return:
        """
        return self.sorted_key_count

    def get_time_index(self):
        """

        :return:
        """
        return self.time_index

    def get_keys(self):
        """

        :return:
        """
        return self.keys

    def log_print(self):
        """

        :return:
        """
        print('GetSubStoreResponse:')
        print('headers:', self.get_all_headers())
        print('substore_name:', self.substore_name)
        print('ttl:', str(self.ttl))
        print('sorted_key_count:', str(self.sorted_key_count))
        print('time_index:', str(self.time_index))
        print('keys:', str(self.keys))


class UpdateSubStoreResponse(LogResponse):
    """ The response of the update_substore API from log.

    :type header: dict
    :param header: UpdateSubStoreResponse HTTP response header
    """

    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('UpdateSubStoreResponse:')
        print('headers:', self.get_all_headers())


class ListSubStoreResponse(LogResponse):
    """ The response of the list_substore API from log.

    :type header: dict
    :param header: ListSubStoreResponse HTTP response header

    :type resp: dict
    :param resp: the HTTP response body
    """

    def __init__(self, resp, header):
        LogResponse.__init__(self, header, resp)
        self._substores = Util.convert_unicode_to_str(resp.get("substores", []))

    def get_substores(self):
        """

        :return:
        """
        return self.substores

    def log_print(self):
        """

        :return:
        """
        print('ListSubStoreResponse:')
        print('headers:', self.get_all_headers())
        print('substores:', str(self._substores))

    @property
    def substores(self):
        return self._substores


class GetSubStoreTTLResponse(LogResponse):
    """ The response of the get_substore_ttl API from log.

    :type header: dict
    :param header: GetSubStoreTTLResponse HTTP response header

    :type resp: dict
    :param resp: the HTTP response body
    """

    def __init__(self, resp, header):
        LogResponse.__init__(self, header, resp)
        self.ttl = int(resp["ttl"])

    def get_ttl(self):
        """

        :return:
        """
        return self.ttl

    def log_print(self):
        print('GetSubStoreTTLResponse:')
        print('ttl:', self.ttl)
        print('headers:', self.get_all_headers())


class UpdateSubStoreTTLResponse(LogResponse):
    """ The response of the update_substore_ttl API from log.

    :type header: dict
    :param header: UpdateSubStoreTTLResponse HTTP response header
    """

    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('UpdateSubStoreTTLResponse:')
        print('headers:', self.get_all_headers())


class CreateMetricsStoreResponse:
    """ The response of the create_metric_store API from log.

    :type header: dict
    :param header: CreateMetricsStoreResponse HTTP response header
    """

    def __init__(self, logstore_response, substore_response):
        self.logstore_response = logstore_response
        self.substore_response = substore_response

    def get_logstore_response(self):
        """

        :return:
        """
        return self.logstore_response

    def get_substore_response(self):
        """

        :return:
        """
        return self.substore_response

    def log_print(self):
        print('CreateLogStoreResponse:')
        print('headers:', self.logstore_response.get_all_headers())
        print('CreateSubStoreResponse:')
        print('headers:', self.substore_response.get_all_headers())
