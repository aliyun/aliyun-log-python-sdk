#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

from .util import Util
from .logresponse import LogResponse

__all__ = ['ListShardResponse', 'DeleteShardResponse']


class ListShardResponse(LogResponse):
    """ The response of the list_shard API from log.
    
    :type header: dict
    :param header: ListShardResponse HTTP response header

    :type resp: dict
    :param resp: the HTTP response body
    """

    def __init__(self, resp, header):
        LogResponse.__init__(self, header, resp)
        self.shards = Util.convert_unicode_to_str(resp)

    def get_shards_info(self):
        return self.shards

    def log_print(self):
        print('ListShardResponse:')
        print('headers:', self.get_all_headers())
        print("res:", self.shards)


class DeleteShardResponse(LogResponse):
    """ The response of the create_logstore API from log.
    
    :type header: dict
    :param header: DeleteShardResponse HTTP response header
    """

    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('DeleteShardResponse:')
        print('headers:', self.get_all_headers())
