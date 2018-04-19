#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.


class ExternalStoreConfig(object):
    """

    """
    def __init__(self,externalStoreName,region,storeType,vpcId,instanceId,host,port,username,password,database,table):
        self.externalStoreName = externalStoreName;
        self.storeType = storeType;
        self.vpcId = vpcId
        self.instanceId = instanceId
        self.host = host
        self.port = port
        self.username=username
        self.password = password
        self.database = database
        self.table = table
        self.region = region

    def to_json(self):
        json_value = {}
        json_value["externalStoreName"] = self.externalStoreName;
        json_value["storeType"] = self.storeType
        param = {}
        param["vpc-id"] = self.vpcId
        param["instance-id"] = self.instanceId
        param["host"] = self.host
        param["port"] = self.port
        param["username"] = self.username
        param["password"] = self.password
        param["db"]  = self.database
        param["table"] = self.table
        param["region"] = self.region
        json_value["parameter"] = param;
        return json_value
    def from_json(self,json_value) : 
        if("externalStoreName" in json_value):
            self.externalStoreName = json_value["externalStoreName"]
        self.storeType = json_value["storeType"]
        self.vpcId = json_value["parameter"]["vpc-id"]
        self.instanceId = json_value["parameter"]["instance-id"]
        self.host = json_value["parameter"]["host"]
        self.port = json_value["parameter"]["port"]
        self.username= json_value["parameter"]["username"]
        self.password = "*"
        self.database = json_value["parameter"]["db"]
        self.table = json_value["parameter"]["table"]
        self.region = json_value["parameter"]["region"]
    def log_print(self):
        print("region",self.region)
        print ("storeType",self.storeType)
        print("vpcId",self.vpcId)
        print("instanceId",self.instanceId)
        print("host",self.host)
        print("port",self.port)
        print("username",self.username)
        print("password",self.password)
        print("db",self.database)
        print("table",self.table)
