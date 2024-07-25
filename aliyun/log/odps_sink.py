from aliyun.log.sink import DataSink, DataSinkType


class AliyunMaxComputeSink(DataSink):
    def __init__(self, partitionColumn, fields):
        super(AliyunMaxComputeSink, self).__init__(DataSinkType.ALIYUN_MaxCompute)
        self.__params = {
            "fields": fields,
            "partitionColumn": partitionColumn,
        }

    def getOdpsAccessKeyId(self):
        return self.__params["odpsAccessKeyId"]

    def setOdpsAccessKeyId(self, odpsAccessKeyId):
        self.__params["odpsAccessKeyId"] = odpsAccessKeyId

    def getOdpsAccessSecret(self):
        return self.__params["odpsAccessSecret"]

    def setOdpsAccessSecret(self, odpsAccessSecret):
        self.__params["odpsAccessSecret"] = odpsAccessSecret

    def getOdpsRolearn(self):
        return self.__params["odpsRolearn"]

    def setOdpsRolearn(self, odpsRolearn):
        self.__params["odpsRolearn"] = odpsRolearn

    def getOdpsEndpoint(self):
        return self.__params["odpsEndpoint"]

    def setOdpsEndpoint(self, odpsEndpoint):
        self.__params["odpsEndpoint"] = odpsEndpoint

    def getOdpsTunnelEndpoint(self):
        return self.__params["odpsTunnelEndpoint"]

    def setOdpsTunnelEndpoint(self, odpsTunnelEndpoint):
        self.__params["odpsTunnelEndpoint"] = odpsTunnelEndpoint

    def getOdpsProject(self):
        return self.__params["odpsProject"]

    def setOdpsProject(self, odpsProject):
        self.__params["odpsProject"] = odpsProject

    def getOdpsTable(self):
        return self.__params["odpsTable"]

    def setOdpsTable(self, odpsTable):
        self.__params["odpsTable"] = odpsTable

    def getTimeZone(self):
        return self.__params["timeZone"]

    def setTimeZone(self, timeZone):
        self.__params["timeZone"] = timeZone

    def getPartitionTimeFormat(self):
        return self.__params["partitionTimeFormat"]

    def setPartitionTimeFormat(self, partitionTimeFormat):
        self.__params["partitionTimeFormat"] = partitionTimeFormat

    def getFields(self):
        return self.__params["fields"]

    def setFields(self, fields):
        self.__params["fields"] = fields

    def getPartitionColumn(self):
        return self.__params["partitionColumn"]

    def setPartitionColumn(self, partitionColumn):
        self.__params["partitionColumn"] = partitionColumn

    def getParams(self):
        return dict(self.__params, **{
            "type": self.getType()
        })
