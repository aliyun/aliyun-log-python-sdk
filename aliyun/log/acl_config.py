#!/usr/bin/env python
#encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

try:
    import json
except ImportError:
    import simplejson as json


class AclConfig:
    def __init__(self, aliyun_id, privilege_list = []):
        """ The acl config
        :type aliyun_id : string
        :param aliyun_id : the aliyun id e.g  "12312313131"

        :type privilege_list : string list
        :param privilege_list : the privilege list array, e.g ["WRITE", "READ", "ADMIN", "LIST"]
        """
        self.principle = aliyun_id
        self.privilege_list = privilege_list

    def to_json(self) : 
        json_value = {}
        json_value['principle'] = self.principle
        json_value['privilege'] = self.privilege_list
        return json_value

    def from_json(self, json_value) : 
        self.principle = json_value['principle']
        self.privilege_list = json_value['privilege']

    
