#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

import time

from .util import Util


class MachineGroupDetail(object):
    """ The machine group detail info

    :type group_name: string
    :param group_name: group name

    :type machine_type: string
    :param machine_type: "ip" or "userdefined"

    :type machine_list: string list
    :param machine_list: the list of machine ips or machine userdefined, e.g ["127.0.0.1", "127.0.0.2"]

    :type group_type: string
    :param group_type: the machine group type, "" or "Armory"

    :type group_attribute: dict
    :param group_attribute: the attributes in group, it contains two optional key :
            1. "externalName": only used if the group_type is "Armory", its the Armory name
            2. "groupTopic": group topic value
    """

    def __init__(self, group_name, machine_type, machine_list, group_type="", group_attribute=None):
        if group_attribute is None:
            group_attribute = {}
        self.group_name = group_name
        self.machine_type = machine_type
        self.machine_list = machine_list
        self.group_type = group_type
        self.group_attribute = group_attribute
        self.create_time = int(time.time())
        self.last_modify_time = self.create_time

    def to_json(self):
        json_value = {'groupName': self.group_name, 'groupType': self.group_type,
                      'groupAttribute': self.group_attribute, 'machineIdentifyType': self.machine_type,
                      'machineList': self.machine_list}
        return json_value

    def from_json(self, json_value):
        self.group_name = json_value.get("groupName", None)
        self.group_type = json_value.get("groupType", "")
        self.group_attribute = json_value.get("groupAttribute", {})
        self.machine_type = json_value.get("machineIdentifyType", None)
        self.machine_list = json_value.get("machineList", None)
        self.create_time = json_value.get("crateTime", None)
        self.last_modify_time = json_value.get("lastModifyTime", None)


class MachineStatus(object):
    """ the machine status
    :type ip: string
    :param ip: the machine ip

    :type machine_unique_id: string
    :param machine_unique_id: the machine unique id
    
    :type user_defined_id: string
    :param user_defined_id: the user defined id

    :type heartbeat_time: int
    :param heartbeat_time: last updated heartbeat_time
    """

    def __init__(self, ip, machine_unique_id, user_defined_id, heartbeat_time):
        self.ip = ip
        self.machine_unique_id = machine_unique_id
        self.user_defined_id = user_defined_id
        self.heartbeat_time = heartbeat_time
