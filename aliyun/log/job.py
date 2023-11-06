import abc
from aliyun.log.sink import DataSink


class JobScheduleType:
    FIXED_RATE = "FixedRate"
    HOURLY = "Hourly"
    DAILY = "Daily"
    WEEKLY = "Weekly"
    CRON = "Cron"
    DRY_RUN = "DryRun"
    RESIDENT = "Resident"


class JobType:
    ALERT = "Alert"
    REPORT = "Report"
    ETL = "ETL"
    INGESTION = "Ingestion"
    REBUILD_INDEX = "RebuildIndex"
    AUDIT_JOB = "AuditJob"
    EXPORT = "Export"
    SCHEDULED_SQL = "ScheduledSQL"


class ExportConfiguration(object):
    def __init__(self):
        self.__version = "v2.0"

    def getToTime(self):
        return self.__toTime

    def setToTime(self, toTime):
        self.__toTime = toTime

    def getVersion(self):
        return self.__version

    # def setVersion(self, version):
    #     self.__version = version

    def getLogstore(self):
        return self.__logstore

    def setLogstore(self, logstore):
        self.__logstore = logstore

    def getAccessKeyId(self):
        return self.__accessKeyId

    def setAccessKeyId(self, accessKeyId):
        self.__accessKeyId = accessKeyId

    def getAccessKeySecret(self):
        return self.__accessKeySecret

    def setAccessKeySecret(self, accessKeySecret):
        self.__accessKeySecret = accessKeySecret

    def getRoleArn(self):
        return self.__roleArn

    def setRoleArn(self, roleArn):
        self.__roleArn = roleArn

    def getInstanceType(self):
        return self.__instanceType

    def setInstanceType(self, instanceType):
        self.__instanceType = instanceType

    def getFromTime(self):
        return self.__fromTime

    def setFromTime(self, fromTime):
        self.__fromTime = fromTime

    def getSink(self):
        return self.__sink

    def setSink(self, sink):
        self.__sink = sink

    def getParameters(self):
        return self.__parameters

    def setParameters(self, parameters):
        self.__parameters = parameters


class AbstractJob(object):

    def getName(self):
        return self.__name

    def setName(self, name):
        self.__name = name

    def getDisplayName(self):
        return self.__displayName

    def setDisplayName(self, displayName):
        self.__displayName = displayName

    def getDescription(self):
        return self.__description

    def setDescription(self, description):
        self.__description = description

    def getType(self):
        return self.__type

    def setType(self, type):
        self.__type = type

    def getRecyclable(self):
        return self.recyclable if hasattr(self, "recyclable") else False

    def setRecyclable(self, recyclable):
        self.recyclable = recyclable

    def getCreateTime(self):
        return self.__createTime

    def setCreateTime(self, createTime):
        self.__createTime = createTime

    def getLastModifiedTime(self):
        return self.__lastModifiedTime

    def setLastModifiedTime(self, lastModifiedTime):
        self.__lastModifiedTime = lastModifiedTime


class JobSchedule(object):
    def __init__(self):
        self.__id = None
        self.__displayName = None
        self.__jobName = None
        self.__type = None
        self.__interval = None
        self.__cronExpression = None
        self.__delay = None
        self.__dayOfWeek = None
        self.__hour = None
        self.__timeZone = None

    def jobScheduleToDict(self):
        config = {
            'id': self.getId(),
            'displayName': self.getDisplayName(),
            'jobName': self.getJobName(),
            'type': self.getType(),
            'interval': self.getInterval(),
            'cronExpression': self.getCronExpression(),
            'delay': self.getDelay(),
            'dayOfWeek': self.getDayOfWeek(),
            'hour': self.getHour(),
            'timeZone': self.getTimeZone()
        }
        return config

    def getId(self):
        return self.__id

    def setId(self, id):
        self.__id = id

    def getDisplayName(self):
        return self.__displayName

    def setDisplayName(self, displayName):
        self.__displayName = displayName

    def getJobName(self):
        return self.__jobName

    def setJobName(self, jobName):
        self.__jobName = jobName

    def getType(self):
        return self.__type

    def setType(self, type):
        self.__type = type

    def getInterval(self):
        return self.__interval

    def setInterval(self, interval):
        self.__interval = interval

    def getCronExpression(self):
        return self.__cronExpression

    def setCronExpression(self, cronExpression):
        self.__cronExpression = cronExpression

    def getDelay(self):
        return self.__delay

    def setDelay(self, delay):
        self.__delay = delay

    def getDayOfWeek(self):
        return self.__dayOfWeek

    def setDayOfWeek(self, dayOfWeek):
        self.__dayOfWeek = dayOfWeek

    def getHour(self):
        return self.__hour

    def setHour(self, hour):
        self.__hour = hour

    def getStatus(self):
        return self.__status

    def setStatus(self, status):
        self.__status = status

    def getCreateTime(self):
        return self.__createTime

    def setCreateTime(self, createTime):
        self.__createTime = createTime

    def getLastModifiedTime(self):
        return self.__lastModifiedTime

    def setLastModifiedTime(self, lastModifiedTime):
        self.__lastModifiedTime = lastModifiedTime

    def getStartTime(self):
        return self.__startTime

    def setStartTime(self, startTime):
        self.__startTime = startTime

    def getCompleteTime(self):
        return self.__completeTime

    def setCompleteTime(self, completeTime):
        self.__completeTime = completeTime

    def getFromTime(self):
        return self.__fromTime

    def setFromTime(self, fromTime):
        self.__fromTime = fromTime

    def getToTime(self):
        return self.__toTime

    def setToTime(self, toTime):
        self.__toTime = toTime

    def getDescription(self):
        return self.__description

    def setDescription(self, description):
        self.__description = description

    def isRunImmediately(self):
        return self.runImmediately if hasattr(self, "runImmediately") else False

    def setRunImmediately(self, runImmediately):
        self.runImmediately = runImmediately

    def getTimeZone(self):
        return self.__timeZone

    def setTimeZone(self, timeZone):
        self.__timeZone = timeZone


class ScheduledJob(AbstractJob):
    def __init__(self):
        self.__status = None
        self.__schedule = JobSchedule()

    def getStatus(self):
        return self.__status

    def setStatus(self, status):
        self.__status = status

    def getSchedule(self):
        return self.__schedule

    def setSchedule(self, schedule):
        self.__schedule = schedule


class Export(ScheduledJob):
    def __init__(self):
        self.configuration = None
        self.setType(JobType.EXPORT)
        schedule = JobSchedule()
        schedule.setType(JobScheduleType.RESIDENT)
        self.setSchedule(schedule)

    def setConfiguration(self, configuration):
        self.configuration = configuration

    def getConfiguration(self):
        return self.configuration

    def getScheduleId(self):
        return self.scheduleId
