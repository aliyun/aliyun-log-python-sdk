# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.
import logging
from .logexception import LogException
import os

logger = logging.getLogger(__name__)

class ExternalStoreConfigBase(object):
    def __init__(self, externalStoreName, storeType):
        pass

    def to_json(self):
        pass

    @staticmethod
    def from_json(json_value):
        storeType = json_value["storeType"]
        if storeType == 'rds-vpc':
            return ExternalStoreConfig.from_json(json_value)
        elif storeType == 'oss':
            return ExternalStoreOssConfig.from_json(json_value)
        elif storeType == 'csv':
            return ExternalStoreCsvConfig.from_json(json_value)
        else:
            raise LogException("Unknown storeType", "please contact support")

    def log_print(self):
        pass


class ExternalStoreConfig(ExternalStoreConfigBase):
    """

    """

    def __init__(self, externalStoreName, region, storeType, vpcId, instanceId, host, port, username, password,
                 database, table, timezone=""):
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
        self.timezone = timezone

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
        param["timezone"] = self.timezone
        json_value["parameter"] = param
        return json_value

    @staticmethod
    def from_json(json_value):
        storeType = json_value["storeType"]
        externalStoreName = json_value["externalStoreName"]
        vpcId = json_value["parameter"].get("vpc-id", "")
        instanceId = json_value["parameter"].get("instance-id", "")
        host = json_value["parameter"].get("host", "")
        port = json_value["parameter"]["port"]
        username = json_value["parameter"]["username"]
        password = json_value["parameter"].get("password", "")
        database = json_value["parameter"]["db"]
        table = json_value["parameter"]["table"]
        region = json_value["parameter"]["region"]
        timezone = json_value["parameter"].get("timezone", "")

        return ExternalStoreConfig(externalStoreName, region, storeType, vpcId, instanceId,
                                   host, port, username, password, database, table, timezone)

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
        print("timezone", self.timezone)


class ExternalStoreOssConfig(ExternalStoreConfigBase):
    """

    """

    def __init__(self, externalStoreName, objects, columns, endpoint, bucket, accessid, accesskey):
        self.externalStoreName = externalStoreName
        self.storeType = 'oss'
        self.objects = objects
        self.columns = columns
        self.bucket = bucket
        self.accessid = accessid
        self.accesskey = accesskey
        self.endpoint = endpoint

    def to_json(self):
        json_value = {}
        json_value["externalStoreName"] = self.externalStoreName
        json_value["storeType"] = self.storeType
        param = {}
        param["endpoint"] = self.endpoint
        param["bucket"] = self.bucket
        param["accessid"] = self.accessid
        param["accesskey"] = self.accesskey
        param["columns"] = self.columns
        param["objects"] = self.objects
        json_value["parameter"] = param
        return json_value

    @staticmethod
    def from_json(json_value):
        externalStoreName = json_value["externalStoreName"]
        storeType = json_value["storeType"]
        endpoint = json_value["parameter"]["endpoint"]
        bucket = json_value["parameter"]["bucket"]
        accessid = json_value["parameter"]["accessid"]
        accesskey = json_value["parameter"]["accesskey"]
        columns = json_value["parameter"]["columns"]
        objects = json_value["parameter"]["objects"]

        return ExternalStoreOssConfig(externalStoreName, objects, columns,
                                      endpoint, bucket, accessid, accesskey)

    def log_print(self):
        print("externalStoreName", self.externalStoreName)
        print ("storeType", self.storeType)
        print("endpoint", self.endpoint)
        print("bucket", self.bucket)
        print("accessid", self.accessid)
        print("accesskey", self.accesskey)
        print("columns", self.columns)
        print("objects", self.objects)

class ExternalStoreCsvConfig(ExternalStoreConfigBase):
    def __init__(self, externalStoreName, externalStoreCsvFile, columns, resp = False):
        import zlib
        import base64
        self.externalStoreName = externalStoreName
        self.columns = columns
        self.objects = [externalStoreCsvFile,]
        self.storeType = 'csv'
        if type(externalStoreCsvFile) == str:
            resp = False
            if externalStoreCsvFile.startswith("file://"):
                externalStoreCsvFile = externalStoreCsvFile.replace("file://", "")
            if not os.path.exists(externalStoreCsvFile):
                raise LogException("ExternalStoreCsvConfig", "The csv file path is not exist")
        if not resp:
            externalStoreCsvFh = open(externalStoreCsvFile, 'rb')
            externalStoreCsv = externalStoreCsvFh.read()
            externalStoreCsvFh.close()
            self.externalStoreCsvSize = len(externalStoreCsv)
            maxOriSize = 50 * 1000 * 1000
            maxSize = 10 * 1000 * 1000  - 10 * 1000
            if self.externalStoreCsvSize > maxOriSize:
                # too large
                raise LogException("ExternalStoreCsvConfig", "csv file size too large, max " + str(maxOriSize))
            externalStoreCsvCompressed = zlib.compress(externalStoreCsv)
            self.externalStoreCsvCompressedBase64 = base64.standard_b64encode(externalStoreCsvCompressed).decode()
            if len(self.externalStoreCsvCompressedBase64) > maxSize:
                raise LogException("ExternalStoreCsvConfig",
                                   "size of csv file after compressed too large, max " + str(maxSize)
                                   + "now size: " + str(len(self.externalStoreCsvCompressedBase64)))

    def to_json(self):
        json_value = {}
        json_value["externalStoreName"] = self.externalStoreName
        json_value["storeType"] = self.storeType
        param = {}
        param["columns"] = self.columns
        param["objects"] = self.objects
        param["externalStoreCsv"] = self.externalStoreCsvCompressedBase64
        param["externalStoreCsvSize"] = self.externalStoreCsvSize
        json_value["parameter"] = param
        return json_value

    @staticmethod
    def from_json(json_value):
        externalStoreName = json_value["externalStoreName"]
        columns = json_value["parameter"]["columns"]
        objects = json_value["parameter"]["objects"]

        return ExternalStoreCsvConfig(externalStoreName, objects, columns, True)

    def log_print(self):
        print("externalStoreName", self.externalStoreName)
        print("storeType", self.storeType)
        print("columns", self.columns)
        #print("objects", self.objects)
