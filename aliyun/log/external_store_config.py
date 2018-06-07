# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.


class ExternalStoreConfig(object):
    """

    """

    def __init__(self, externalStoreName, region, storeType, vpcId, instanceId, host, port, username, password,
                 database, table):
        self.externalStoreName = externalStoreName
        self.storeType = storeType
        self.vpcId = vpcId
        self.instanceId = instanceId
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.table = table
        self.region = region

    def to_json(self):
        json_value = {}
        json_value["externalStoreName"] = self.externalStoreName
        json_value["storeType"] = self.storeType
        param = {}
        param["vpc-id"] = self.vpcId
        param["instance-id"] = self.instanceId
        param["host"] = self.host
        param["port"] = self.port
        param["username"] = self.username
        param["password"] = self.password
        param["db"] = self.database
        param["table"] = self.table
        param["region"] = self.region
        json_value["parameter"] = param
        return json_value

    @staticmethod
    def from_json(json_value):
        externalStoreName = json_value["externalStoreName"]
        storeType = json_value["storeType"]
        vpcId = json_value["parameter"]["vpc-id"]
        instanceId = json_value["parameter"]["instance-id"]
        host = json_value["parameter"]["host"]
        port = json_value["parameter"]["port"]
        username = json_value["parameter"]["username"]
        password = json_value["parameter"].get("password", "")
        database = json_value["parameter"]["db"]
        table = json_value["parameter"]["table"]
        region = json_value["parameter"]["region"]

        return ExternalStoreConfig(externalStoreName, region, storeType, vpcId, instanceId,
                                   host, port, username, password, database, table)

    def log_print(self):
        print("region", self.region)
        print ("storeType", self.storeType)
        print("vpcId", self.vpcId)
        print("instanceId", self.instanceId)
        print("host", self.host)
        print("port", self.port)
        print("username", self.username)
        print("password", self.password)
        print("db", self.database)
        print("table", self.table)
