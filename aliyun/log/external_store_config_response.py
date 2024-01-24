# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

__all__ = ['CreateExternalStoreResponse', 'DeleteExternalStoreResponse', 'GetExternalStoreResponse',
           'UpdateExternalStoreResponse', 'ListExternalStoreResponse']

from .util import Util
from .logresponse import LogResponse
from .external_store_config import ExternalStoreConfigBase


class CreateExternalStoreResponse(LogResponse):
    """ The response of the create_logstore API from log.

    :type header: dict
    :param header: CreateExternalStoreResponse HTTP response header
    """

    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('CreateExternalStoreResponse:')
        print('headers:', self.get_all_headers())


class DeleteExternalStoreResponse(LogResponse):
    """ The response of the delete_logstore API from log.

    :type header: dict
    :param header: DeleteExternalStoreResponse HTTP response header
    """

    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('DeleteExternalStoreResponse:')
        print('headers:', self.get_all_headers())


class GetExternalStoreResponse(LogResponse):
    """ The response of the get_logstore API from log.

    :type header: dict
    :param header: GetExternalStoreResponse HTTP response header

    :type resp: dict
    :param resp: the HTTP response body
    """

    def __init__(self, resp, header):
        LogResponse.__init__(self, header, resp)
        self.externalStoreConfig = ExternalStoreConfigBase.from_json(resp)

    def get_external_store_config(self):
        """

        :return:
        """
        return self.externalStoreConfig

    def log_print(self):
        """

        :return:
        """
        print('GetExternalStoreResponse:')
        print('headers:', self.get_all_headers())
        self.externalStoreConfig.log_print()


class UpdateExternalStoreResponse(LogResponse):
    """ The response of the update_logstore API from log.

    :type header: dict
    :param header: UpdateExternalStoreResponse HTTP response header
    """

    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('UpdateExternalStoreResponse:')
        print('headers:', self.get_all_headers())
        print('body:', self.get_body())


class ListExternalStoreResponse(LogResponse):
    """ The response of the list_logstore API from log.

    :type header: dict
    :param header: ListExternalStoreResponse HTTP response header

    :type resp: dict
    :param resp: the HTTP response body
    """

    def __init__(self, resp, header):
        LogResponse.__init__(self, header, resp)
        self.count = int(resp["count"])
        self.total_count = int(resp["total"])
        self.logstores = Util.convert_unicode_to_str(resp.get("externalstores", []))

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
        print('ListExternalStoreResponse:')
        print('headers:', self.get_all_headers())
        print('logstores_count:', str(self.count))
        print('logstores_total:', str(self.total_count))
        print('logstores:', str(self.logstores))

    def merge(self, response):
        if not isinstance(response, ListExternalStoreResponse):
            raise ValueError("passed response is not a ListLogstoresResponse: " + str(type(response)))

        self.count += response.get_count()
        self.total = response.get_total()  # use the latest total count
        self.logstores.extend(response.get_logstores())

        # update body
        self.body = {
            'count': self.count,
            'total': self.total,
            'logstores': self.logstores
        }

        return self
