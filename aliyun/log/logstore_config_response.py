#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

__all__ = ['CreateLogStoreResponse', 'DeleteLogStoreResponse', 'GetLogStoreResponse',
           'UpdateLogStoreResponse', 'ListLogStoreResponse']

from .util import Util
from .logresponse import LogResponse


class CreateLogStoreResponse(LogResponse):
    """ The response of the create_logstore API from log.

    :type header: dict
    :param header: CreateLogStoreResponse HTTP response header
    """

    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('CreateLogStoreResponse:')
        print('headers:', self.get_all_headers())


class DeleteLogStoreResponse(LogResponse):
    """ The response of the delete_logstore API from log.

    :type header: dict
    :param header: DeleteLogStoreResponse HTTP response header
    """

    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('DeleteLogStoreResponse:')
        print('headers:', self.get_all_headers())


class GetLogStoreResponse(LogResponse):
    """ The response of the get_logstore API from log.

    :type header: dict
    :param header: GetLogStoreResponse HTTP response header

    :type resp: dict
    :param resp: the HTTP response body
    """

    def __init__(self, resp, header):
        LogResponse.__init__(self, header, resp)
        self.logstore_name = Util.convert_unicode_to_str(resp["logstoreName"])
        self.ttl = int(resp["ttl"])
        self.shard_count = int(resp["shardCount"])
        self.enable_tracking = bool(resp["enable_tracking"])
        self.append_meta = bool(resp["appendMeta"])
        self.auto_split = bool(resp["autoSplit"])
        self.max_split_shard = int(resp["maxSplitShard"])
        self.preserve_storage = self.ttl >= 3650
        self.encrypt_conf = None
        self.mode = None
        self.hot_ttl=-1;
        if 'hot_ttl' in resp:
            self.hot_ttl= int(resp['hot_ttl'])
        if 'encrypt_conf' in resp:
            self.encrypt_conf = resp["encrypt_conf"]
        if 'mode' in resp:
            self.mode = resp["mode"]
        if 'telemetryType' in resp:
            self.telemetry_type = resp["telemetryType"]
        if 'infrequentAccessTTL' in resp:
            self.infrequent_access_ttl = resp["infrequentAccessTTL"]

    def get_shard_count(self):
        """

        :return:
        """
        return self.shard_count

    def get_ttl(self):
        """

        :return:
        """
        return self.ttl
    def get_hot_ttl(self):
        """
        :return:
        """
        return self.hot_ttl
    def get_enable_tracking(self):
        """

        :return:
        """
        return self.enable_tracking

    def get_encrypt_conf(self):
        """

        :return:
        """
        return self.encrypt_conf

    def log_print(self):
        """

        :return:
        """
        print('GetLogStoreResponse:')
        print('headers:', self.get_all_headers())
        print('logstore_name:', self.logstore_name)
        print('shard_count:', str(self.shard_count))
        print('ttl:', str(self.ttl))
        if self.encrypt_conf != None:
            print('encrypt_conf:', str(self.encrypt_conf))
        if hasattr(self, 'mode') and self.mode != None:
            print('mode:', str(self.mode))


class UpdateLogStoreResponse(LogResponse):
    """ The response of the update_logstore API from log.

    :type header: dict
    :param header: UpdateLogStoreResponse HTTP response header
    """

    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('UpdateLogStoreResponse:')
        print('headers:', self.get_all_headers())


class ListLogStoreResponse(LogResponse):
    """ The response of the list_logstore API from log.

    :type header: dict
    :param header: ListLogStoreResponse HTTP response header

    :type resp: dict
    :param resp: the HTTP response body
    """

    def __init__(self, resp, header):
        LogResponse.__init__(self, header, resp)
        self.count = int(resp["count"])
        self.total_count = int(resp["total"])
        self._logstores = Util.convert_unicode_to_str(resp.get("logstores", []))

    def get_logstores(self):
        """

        :return:
        """
        return self.logstores

    def get_count(self):
        return self.count

    def get_logstores_count(self):
        """

        :return:
        """
        return self.count

    def get_logstores_total(self):
        """

        :return:
        """
        return self.total_count

    def get_total(self):
        """

        :return:
        """
        return self.total_count

    def log_print(self):
        """

        :return:
        """
        print('ListLogStoreResponse:')
        print('headers:', self.get_all_headers())
        print('logstores_count:', str(self.count))
        print('logstores_total:', str(self.total_count))
        print('logstores:', str(self._logstores))

    def merge(self, response):
        if not isinstance(response, ListLogStoreResponse):
            raise ValueError("passed response is not a ListLogstoresResponse: " + str(type(response)))

        self.count += response.get_count()
        self.total_count = response.get_total() # use the latest total count
        self.logstores.extend(response.get_logstores())

        # update body
        self.body = {
            'count': self.count,
            'total': self.total,
            'logstores': self.logstores
        }

        return self

    @property
    def total(self,):
        return self.total_count

    @property
    def logstores(self):
        return self._logstores
