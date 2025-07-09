#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

__all__ = ['CreateStoreViewResponse', 'UpdateStoreViewResponse', 'DeleteStoreViewResponse', 'ListStoreViewsResponse', 'GetStoreViewResponse']

from .store_view import StoreView
from .logresponse import LogResponse


class CreateStoreViewResponse(LogResponse):
    """ The response of the create_store_view API from log.

    :type header: dict
    :param header: CreateStoreViewResponse HTTP response header
    """

    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('CreateStoreViewResponse:')
        print('request_id:', self.get_request_id())
        print('headers:', self.get_all_headers())


class UpdateStoreViewResponse(LogResponse):
    """ The response of the update_store_view API from log.

    :type header: dict
    :param header: UpdateStoreViewResponse HTTP response header
    """

    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('UpdateStoreViewResponse:')
        print('request_id:', self.get_request_id())
        print('headers:', self.get_all_headers())


class DeleteStoreViewResponse(LogResponse):
    """ The response of the delete_store_view API from log.

    :type header: dict
    :param header: DeleteStoreViewResponse HTTP response header
    """

    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('DeleteStoreViewResponse:')
        print('request_id:', self.get_request_id())
        print('headers:', self.get_all_headers())


class ListStoreViewsResponse(LogResponse):
    """ The response of the list_store_views API from log.

    :type header: dict
    :param header: ListStoreViewsResponse HTTP response header
    """

    def __init__(self, header, resp):
        LogResponse.__init__(self, header, resp)
        self._count = resp['count']
        self._total = resp['total']
        self._store_views = resp['storeviews']

    def get_count(self):
        """Get the count of the store views

        :return: int, the count of the store views
        """
        return self._count
    
    def get_total(self):
        """Get the total of the store views

        :return: int, the total of the store views
        """
        return self._total
    
    def get_store_views(self):
        """Get the store views from the response

        :return: list, the store view names, each item is a str
        """
        return self._store_views

    def log_print(self):
        print('ListStoreViewsResponse:')
        print('request_id:', self.get_request_id())
        print('headers:', self.get_all_headers())
        print('response:', self.get_body())


class GetStoreViewResponse(LogResponse):
    """ The response of the get_store_views API from log.

    :type header: dict
    :param header: GetStoreViewResponse HTTP response header
    """

    def __init__(self, header, resp):
        LogResponse.__init__(self, header, resp)
        self._store_view = StoreView._from_json_dict(resp)

    def get_store_view(self):
        # type: () -> StoreView
        """Get StoreView from the response

        :return: instance of StoreView
        """
        return self._store_view

    def log_print(self):
        print('GetStoreViewResponse:')
        print('request_id:', self.get_request_id())
        print('headers:', self.get_all_headers())
        print('store_view:', self.get_body())
