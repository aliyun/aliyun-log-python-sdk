from aliyun.log.sink import DataSink, DataSinkType


class AliyunOSSSink(DataSink):
    def __init__(self, roleArn, bucket, prefix, suffix, pathFormat, pathFormatType, bufferSize, bufferInterval, timeZone, contentType, compressionType, contentDetail):
        super(AliyunOSSSink, self).__init__(DataSinkType.ALIYUN_OSS)
        self.__roleArn = roleArn
        self.__bucket = bucket
        self.__prefix = prefix
        self.__suffix = suffix
        self.__pathFormat = pathFormat
        self.__pathFormatType = pathFormatType
        self.__bufferSize = bufferSize
        self.__bufferInterval = bufferInterval
        self.__timeZone = timeZone
        self.__contentType = contentType
        self.__compressionType = compressionType
        self.__contentDetail = contentDetail

    def getRoleArn(self):
        return self.__roleArn

    def setRoleArn(self, roleArn):
        self.__roleArn = roleArn

    def getBucket(self):
        return self.__bucket

    def setBucket(self, bucket):
        self.__bucket = bucket

    def getPrefix(self):
        return self.__prefix

    def setPrefix(self, prefix):
        self.__prefix = prefix

    def getSuffix(self):
        return self.__suffix

    def setSuffix(self, suffix):
        self.__suffix = suffix

    def getPathFormat(self):
        return self.__pathFormat

    def setPathFormat(self, pathFormat):
        self.__pathFormat = pathFormat

    def getPathFormatType(self):
        return self.__pathFormatType

    def setPathFormatType(self, pathFormatType):
        self.__pathFormatType = pathFormatType

    def getBufferSize(self):
        return self.__bufferSize

    def setBufferSize(self, bufferSize):
        self.__bufferSize = bufferSize

    def getBufferInterval(self):
        return self.__bufferInterval

    def setBufferInterval(self, bufferInterval):
        self.__bufferInterval = bufferInterval

    def getTimeZone(self):
        return self.__timeZone

    def setTimeZone(self, timeZone):
        self.__timeZone = timeZone

    def getContentType(self):
        return self.__contentType

    def setContentType(self, contentType):
        self.__contentType = contentType

    def getCompressionType(self):
        return self.__compressionType

    def setCompressionType(self, compressionType):
        self.__compressionType = compressionType

    def getContentDetail(self):
        return self.__contentDetail

    def setContentDetail(self, contentDetail):
        self.__contentDetail = contentDetail
