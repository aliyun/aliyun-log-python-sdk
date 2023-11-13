#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

__all__ = ['UpdateAclResponse', 'ListAclResponse']

from .acl_config import AclConfig
from .logresponse import LogResponse


class UpdateAclResponse(LogResponse):
    """ The response of the update_projecta_acl/update_logstore_acl API from log.

    :type header: dict
    :param header: UpdateAclResponse HTTP response header
    """

    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('UpdateAclResponse:')
        print('headers:', self.get_all_headers())


class ListAclResponse(LogResponse):
    """ The response of the get_project_acl/get_logstore_acl API from log.

    :type header: dict
    :param header: GetAclResponse HTTP response header
    """

    def __init__(self, resp, header):
        LogResponse.__init__(self, header, resp)
        self.count = int(resp["count"])
        self.total = int(resp["total"])
        self.acl_list = []
        for acl in resp.get("acls", []):
            acl_config = AclConfig(None, None)
            acl_config.from_json(acl)
            self.acl_list.append(acl_config)

    def get_acl_count(self):
        return self.count

    def get_acl_list(self):
        return self.acl_list

    def log_print(self):
        print('GetLogStoreResponse:')
        print('headers:', self.get_all_headers())
        print('acl_count:', str(self.count))
        print('acl_total:', str(self.total))
        print('acl_list:')
        for acl in self.acl_list:
            print(acl.to_json())
