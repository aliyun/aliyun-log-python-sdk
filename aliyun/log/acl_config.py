#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.


class AclConfig(object):
    def __init__(self, aliyun_id, privilege_list=None):
        """ The acl config
        :type aliyun_id: string
        :param aliyun_id: the aliyun id e.g  "12312313131"

        :type privilege_list: string list
        :param privilege_list: the privilege list array, e.g ["WRITE", "READ", "ADMIN", "LIST"]
        """
        if privilege_list is None:
            privilege_list = []
        self.principle = aliyun_id
        self.privilege_list = privilege_list

    def to_json(self):
        json_value = {'principle': self.principle, 'privilege': self.privilege_list}
        return json_value

    def from_json(self, json_value):
        self.principle = json_value['principle']
        self.privilege_list = json_value['privilege']
