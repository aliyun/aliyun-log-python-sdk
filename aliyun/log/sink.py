

class DataSinkType:
    ALIYUN_LOG = "AliyunLOG"
    ALIYUN_ADB = "AliyunADB"
    ALIYUN_OSS = "AliyunOSS"
    ALIYUN_MaxCompute = "AliyunODPS"
    GENERAL = "GENERAL"


class DataSink(object):
    def __init__(self, type):
        self._type = type

    def getType(self):
        return self._type

    def setType(self, type):
        self._type = type
