from aliyun.log.job import ScheduledJob, JobType, JobSchedule

def dict_to_scheduled_sql_config(dict_config):
    config = ScheduledSQLConfiguration()
    for key, value in dict_config.items():
        setattr(config, '_ScheduledSQLConfiguration__' + key, value)
    return config


def dict_to_schedule_rule(dict_rule):
    rule = JobSchedule()
    for key, value in dict_rule.items():
        setattr(rule, '_JobSchedule__' + key, value)
    return rule


class ScheduledSQL(ScheduledJob):
    def __init__(self):
        self.__scheduleId = ""
        self.__configuration = ScheduledSQLConfiguration()
        self.__schedule = JobSchedule()
        self.setType(JobType.SCHEDULED_SQL)

    def getScheduleId(self):
        return self.__scheduleId

    def setScheduleId(self, scheduleId):
        self.__scheduleId = scheduleId

    def getConfiguration(self):
        return self.__configuration

    def setConfiguration(self, configuration):
        self.__configuration = configuration

    def scheduled_sql_to_dict(self):
        scheduled_sql = {
            "name": self.getName(),
            "displayName": self.getDisplayName(),
            "description": self.getDescription(),
            "schedule": self.getSchedule().jobScheduleToDict(),
            "configuration": self.getConfiguration().configurationToDict(),
            "type": self.getType()
        }
        return scheduled_sql


class ScheduledSQLConfiguration:
    def __init__(self):
        self.__sourceLogstore = None
        self.__destProject = None
        self.__destEndpoint = None
        self.__destLogstore = None
        self.__script = None
        self.__sqlType = "standard"
        self.__resourcePool = "enhanced"
        self.__roleArn = None
        self.__destRoleArn = None
        self.__fromTimeExpr = None
        self.__toTimeExpr = None
        self.__maxRunTimeInSeconds = 900
        self.__maxRetries = 5
        self.__fromTime = 0
        self.__toTime = 0
        self.__dataFormat = "log2log"
        self.__parameters = ScheduledSQLParameters()

    def configurationToDict(self):
        config = {
            "sourceLogstore": self.__sourceLogstore,
            "roleArn": self.__roleArn,
            "destRoleArn": self.__destRoleArn,
            "script": self.__script,
            "sqlType": self.__sqlType,
            "resourcePool": self.__resourcePool,
            "destEndpoint": self.__destEndpoint,
            "destProject": self.__destProject,
            "destLogstore": self.__destLogstore,
            "fromTimeExpr": self.__fromTimeExpr,
            "toTimeExpr": self.__toTimeExpr,
            "maxRunTimeInSeconds": self.__maxRunTimeInSeconds,
            "maxRetries": self.__maxRetries,
            "fromTime": self.__fromTime,
            "toTime": self.__toTime,
            "dataFormat": self.__dataFormat,
            "parameters": self.__parameters if isinstance(self.__parameters, dict) else self.__parameters.toDict(
                self.__dataFormat)
        }
        return config

    def getSourceLogstore(self):
        return self.__sourceLogstore

    def setSourceLogstore(self, sourceLogstore):
        self.__sourceLogstore = sourceLogstore

    def getDestProject(self):
        return self.__destProject

    def setDestProject(self, destProject):
        self.__destProject = destProject

    def getDestEndpoint(self):
        return self.__destEndpoint

    def setDestEndpoint(self, destEndpoint):
        self.__destEndpoint = destEndpoint

    def getDestLogstore(self):
        return self.__destLogstore

    def setDestLogstore(self, destLogstore):
        self.__destLogstore = destLogstore

    def getScript(self):
        return self.__script

    def setScript(self, script):
        self.__script = script

    def getSqlType(self):
        return self.__sqlType

    def setSqlType(self, sqlType):
        self.__sqlType = sqlType

    def getResourcePool(self):
        return self.__resourcePool

    def setResourcePool(self, resourcePool):
        self.__resourcePool = resourcePool

    def getRoleArn(self):
        return self.__roleArn

    def setRoleArn(self, roleArn):
        self.__roleArn = roleArn

    def getDestRoleArn(self):
        return self.__destRoleArn

    def setDestRoleArn(self, destRoleArn):
        self.__destRoleArn = destRoleArn

    def getFromTimeExpr(self):
        return self.__fromTimeExpr

    def setFromTimeExpr(self, fromTimeExpr):
        self.__fromTimeExpr = fromTimeExpr

    def getToTimeExpr(self):
        return self.__toTimeExpr

    def setToTimeExpr(self, toTimeExpr):
        self.__toTimeExpr = toTimeExpr

    def getMaxRunTimeInSeconds(self):
        return self.__maxRunTimeInSeconds

    def setMaxRunTimeInSeconds(self, maxRunTimeInSeconds):
        self.__maxRunTimeInSeconds = maxRunTimeInSeconds

    def getMaxRetries(self):
        return self.__maxRetries

    def setMaxRetries(self, maxRetries):
        self.__maxRetries = maxRetries

    def getFromTime(self):
        return self.__fromTime

    def setFromTime(self, fromTime):
        self.__fromTime = fromTime

    def getToTime(self):
        return self.__toTime

    def setToTime(self, toTime):
        self.__toTime = toTime

    def getDataFormat(self):
        return self.__dataFormat

    def setDataFormat(self, dataFormat):
        self.__dataFormat = dataFormat

    def getParameters(self):
        return self.__parameters

    def setParameters(self, parameters):
        self.__parameters = parameters


class ScheduledSQLParameters:
    def toDict(self, dataFormat):
        if dataFormat.lower() == "log2metric":
            params_dict = self.toDictByType(Log2MetricParameters)
        elif dataFormat.lower() == "metric2metric":
            params_dict = self.toDictByType(Metric2MetricParameters)
        elif dataFormat.lower() == "log2log":
            params_dict = self.toDictByType(ScheduledSQLBaseParameters)
        else:
            raise ValueError("Unsupported dataFormat: {}".format(dataFormat))
        return params_dict

    def toDictByType(self, dateFormatType):
        if isinstance(self, dateFormatType):
            return self.paramsToDict()


class ScheduledSQLBaseParameters(ScheduledSQLParameters):
    def __init__(self):
        self.__baseParams = {}
        self.__fields = set()

    def withFields(self, *fields):
        self.__fields.update(fields)

    def getBaseParams(self):
        return self.__baseParams

    def setBaseParams(self, params):
        if not self.__fields.issuperset(params.keys()):
            return
        self.__baseParams = params

    def addBaseParams(self, key, value):
        if key not in self.__fields:
            return
        self.__baseParams[key] = value

    def paramsToDict(self):
        return self.__baseParams


class Log2MetricParameters(ScheduledSQLBaseParameters):
    def __init__(self):
        super().__init__()
        self.__timeKey = None
        self.__metricKeys = None
        self.__labelKeys = None
        self.__hashLabels = None
        self.__addLabels = None

    def paramsToDict(self):
        params_dict = {
            "timeKey": self.__timeKey,
            "metricKeys": self.__metricKeys,
            "labelKeys": self.__labelKeys,
            "hashLabels": self.__hashLabels,
            "addLabels": self.__addLabels
        }
        return params_dict

    def getTimeKey(self):
        return self.__timeKey

    def setTimeKey(self, time_key):
        self.__timeKey = time_key

    def getMetricKeys(self):
        return self.__metricKeys

    def setMetricKeys(self, metric_keys):
        self.__metricKeys = metric_keys

    def getLabelKeys(self):
        return self.__labelKeys

    def setLabelKeys(self, label_keys):
        self.__labelKeys = label_keys

    def getHashLabels(self):
        return self.__hashLabels

    def setHashLabels(self, hash_labels):
        self.__hashLabels = hash_labels

    def getAddLabels(self):
        return self.__addLabels

    def setAddLabels(self, add_labels):
        self.__addLabels = add_labels


class Metric2MetricParameters(ScheduledSQLBaseParameters):
    def __init__(self):
        super().__init__()
        self.__metricName = None
        self.__addLabels = None
        self.__hashLabels = None

    def paramsToDict(self):
        params_dict = {
            "metricName": self.__metricName,
            "addLabels": self.__addLabels,
            "hashLabels": self.__hashLabels
        }
        return params_dict

    def getMetricName(self):
        return self.__metricName

    def setMetricName(self, metric_name):
        self.__metricName = metric_name

    def getAddLabels(self):
        return self.__addLabels

    def setAddLabels(self, add_labels):
        self.__addLabels = add_labels

    def getHashLabels(self):
        return self.__hashLabels

    def setHashLabels(self, hash_labels):
        self.__hashLabels = hash_labels
