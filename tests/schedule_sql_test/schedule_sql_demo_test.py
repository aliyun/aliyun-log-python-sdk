import time

from aliyun.log import LogClient
from aliyun.log.scheduled_sql import *

accessKeyId = ""  # The AccessKeyId
accessKeySecret = ""  # The AccessKeySecret

endpoint = "cn-shanghai.log.aliyuncs.com"  # The  source endpoint of the project's region
roleArn = ""  # The roleArn
project = "etl-project"  # The source project name
source_logstore = ""  # The source logstore name
source_metricstore = ""  # The source metricstore name

dest_endpoint = ""  # The endpoint of the destination project's region
dest_role_arn = ""  # The destination roleArn
dest_project = ""  # The destination project name
dest_logstore = ""  # The destination logstore name
dest_metricstore = ""  # The destination metricstore name

from_time = int(time.time()) - 360  # The start time of the scheduled SQL task

job_name = "test-001"  # The job name
display_name = "test-001"  # The display name
description = "development"  # The description

script = "* | select * where key > 680"  # The SQL script
promql = "* | select promql_query_range('key{}') from metrics limit 1000"  # The PromQL script

instance_id = ""  # The job instanceId for schedule sql
delay_seconds = 0  # the delay seconds for schedule sql

# three possible values for the variable data_format  : "log2log" , "log2metric" ,"metric2metric"
data_format = "log2log"

# Possible values for the variable schedule_type: "FixedRate", "Daily", "Weekly", "Hourly", "Cron"
# schedule_type = "FixedRate"
schedule_type = "FixedRate"

client = LogClient(endpoint, accessKeyId, accessKeySecret)


def generate_schedule_sql():
    schedule_sql = ScheduledSQL()

    schedule_rule = generate_schedule_rule()
    parameters = generate_params()

    query = script
    if data_format == 'log2log':
        source = source_logstore
        dest = dest_logstore
    elif data_format == 'log2metric':
        source = source_logstore
        dest = dest_metricstore
    elif data_format == 'metric2metric':
        source = source_metricstore
        dest = dest_metricstore
        query = promql

    config = ScheduledSQLConfiguration()
    config.setRoleArn(roleArn)
    config.setSqlType('searchQuery')
    config.setDestEndpoint(dest_endpoint)
    config.setDestProject(dest_project)
    config.setSourceLogstore(source)
    config.setDestLogstore(dest)
    config.setDestRoleArn(dest_role_arn)
    config.setScript(query)
    config.setFromTimeExpr("@m - 5m")
    config.setToTimeExpr("@m")
    config.setMaxRunTimeInSeconds(1800)
    config.setResourcePool("enhanced")
    config.setMaxRetries(10)
    config.setFromTime(from_time)
    config.setToTime(0)
    config.setDataFormat(data_format)
    config.setParameters(parameters)

    schedule_sql.setSchedule(schedule_rule)
    schedule_sql.setConfiguration(config)
    schedule_sql.setName(job_name)
    schedule_sql.setDisplayName(display_name)
    schedule_sql.setDescription("schedule-sql-demo")
    return schedule_sql


def generate_schedule_rule():
    schedule_rule = JobSchedule()
    schedule_rule.setDelay(delay_seconds)
    schedule_rule.setTimeZone('+0800')
    schedule_rule.setType(schedule_type)

    if schedule_type == "FixedRate":
        # The interval value must be a string in the format "Xs", "Xm", "Xh", or "Xd",
        # where X is an integer representing the number of seconds, minutes, hours, or days.
        schedule_rule.setInterval('60s')
    elif schedule_type == "Cron":
        # The cron expression must be a valid cron string.
        schedule_rule.setCronExpression("*/2 * * * *")
    elif schedule_type == "Daily":
        # The hour value must be an integer between 0 and 23.
        schedule_rule.setHour(1)
    elif schedule_type == "Weekly":
        # The day of the week value must be an integer between 1 and 7.
        # The hour value must be an integer between 0 and 23.
        schedule_rule.setDayOfWeek(1)
        schedule_rule.setHour(2)
    elif schedule_type == "Hourly":
        pass

    return schedule_rule


def generate_params():
    if data_format.lower() == "log2log":
        params = generate_log2log_params()
        return params
    elif data_format.lower() == "log2metric":
        params = generate_log2metric_params()
        return params
    elif data_format.lower() == "metric2metric":
        params = generate_metric2metric_params()
        return params
    else:
        return {}


def generate_log2log_params():
    parameters = ScheduledSQLBaseParameters()
    return parameters


def generate_log2metric_params():
    parameters = Log2MetricParameters()
    parameters.setMetricKeys("[\"metric_key1\",\"metric_key2\"]")
    parameters.setLabelKeys("[\"label_key1\",\"label_key2\",\"label_key3\"]")
    # if no time key need to be set , set "" to parameters.setTimeKey("")
    parameters.setTimeKey("start_time")
    parameters.setAddLabels("{\"demo1\":\"demo1_value\",\"demo2\":\"demo2_value\"}")
    parameters.setHashLabels("[\"label_key1\",\"label_key2\"]")
    return parameters


def generate_metric2metric_params():
    parameters = Metric2MetricParameters()
    parameters.setMetricName("demo")
    parameters.setHashLabels("[\"label_key1\",\"label_key2\"]")
    parameters.setAddLabels("{\"demo3\":\"demo3_value\",\"demo4\":\"demo4_value\"}")
    return parameters


def modify_schedule_sql_instance_state():
    # Modify the state of a scheduled sql job instance to RUNNING to re-execute it.
    # Note: The job state can only be set to RUNNING.
    job_state = "RUNNING"
    modify_schedule_sql_instance_state_response = client.modify_scheduled_sql_job_instance_state(project, job_name,
                                                                                                 instance_id, job_state)

    modify_schedule_sql_instance_state_response.log_print()


def get_schedule_sql_instance():
    get_schedule_sql_instance_response = client.get_scheduled_sql_job_instance(project, job_name,
                                                                               instance_id)
    instance = get_schedule_sql_instance_response.get_instance()
    print(instance)


def create_schedule_sql_with_dict():
    """
    in the config below

    If `dataformat` is set to `log2log`, then `parameters` dict should be an empty {}.
     For example
    "dataFormat": "log2metric",
    "parameters": {}


    If `dataformat` is set to `log2metric`, then `parameters` dict should contain the following keys:
    - `labelKeys`: a list of strings representing the labels to be used in the metric
    - `metricKeys`: a list of strings representing the keys to be used in the metric
    - `addLabels`: a dictionary of additional labels to be added to the metric
    - `hashLabels`: a list of labels to be hashed
    - `timeKey`: a string representing the key to be used as the timestamp in the metric

    For example
    "dataFormat": "log2metric",
    "parameters": {
        "metricKeys":"[\"seq\"]",
        "labelKeys":"[\"cdn_in\",\"cdn_out\",\"intranet_out\"]",
        "hashLabels":"[\"bucket_location\"]",
        "timeKey":"",
        "addLabels":"{\"demo1\":\"demo1_value\",\"demo2\":\"demo2_value\"}",
        "metricName":""
    }


    If `dataformat` is set to `metric2metric`, then `parameters` dict should contain the following keys:
    - `addLabels`: a dictionary of additional labels to be added to the metric
    - `hashLabels`: a list of labels to be hashed
    - `metricName`: a string representing the name of the metric

    For example
    "dataFormat": "metric2metric",
    "parameters": {
        "addLabels":"{\"demo3\":\"demo3_value\",\"demo4\":\"demo4_value\"}",
        "hashLabels":"[\"demo1\"]",
        "metricName":"demo"
    }

    other schedule_rule will be like :
    dict_schedule_rule = {
        'type': 'Daily',
        "delay": delay_seconds,
        "hour": 1,
        "timeZone": "+0800"
    }

    dict_schedule_rule = {
        'type': 'Weekly',
        "delay": delay_seconds,
        "timeZone": "+0800",
        "dayOfWeek": 1,
        "hour": 0
    }
    """

    dict_config = {
        'roleArn': roleArn,
        "sqlType": "searchQuery",
        "destEndpoint": dest_endpoint,
        "destProject": dest_project,
        'destLogstore': dest_logstore,
        "sourceLogstore": source_logstore,
        "destRoleArn": dest_role_arn,
        'script': script,
        "fromTimeExpr": "@m-5m",
        "toTimeExpr": "@m",
        "maxRunTimeInSeconds": 2400,
        'resourcePool': "enhanced",
        "maxRetries": 10,
        'fromTime': from_time,
        "dataFormat": "log2log",
        "parameters": {},
        'toTime': 0,
    }

    dict_schedule_rule = {
        'type': 'FixedRate',
        "delay": delay_seconds,
        "interval": "1m",
        "timeZone": "+0800"
    }

    schedule_rule = dict_to_schedule_rule(dict_schedule_rule)
    config = dict_to_scheduled_sql_config(dict_config)

    schedule_sql = ScheduledSQL()
    schedule_sql.setSchedule(schedule_rule)
    schedule_sql.setConfiguration(config)
    schedule_sql.setName(job_name)
    schedule_sql.setDisplayName(display_name)
    schedule_sql.setDescription(description)

    client.create_scheduled_sql(project, schedule_sql)


def list_schedule_sql_instances():
    now = int(time.time())
    begin_time = now - 30 * 60
    to_time = now + 30 * 60

    list_schedule_sql_instances_response = client.list_scheduled_sql_job_instance(project, job_name,
                                                                                  begin_time, to_time)
    instances_list = list_schedule_sql_instances_response.get_scheduled_sql_jobs()
    for instance in instances_list:
        print(instance)


def create_schedule_sql():
    schedule_sql = generate_schedule_sql()

    client.create_scheduled_sql(project, schedule_sql)


def get_schedule_sql():
    get_schedule_sql_response = client.get_scheduled_sql(project, job_name)
    scheduled_sql_config = get_schedule_sql_response.scheduled_sql_config
    print(scheduled_sql_config)
    return scheduled_sql_config


def list_schedule_sql():
    list_schedule_sql_response = client.list_scheduled_sql(project)
    schedule_sql_jobs = list_schedule_sql_response.get_scheduled_sql_jobs()
    for job in schedule_sql_jobs:
        print(job)


def delete_schedule_sql():
    delete_schedule_sql_response = client.delete_scheduled_sql(project, job_name)
    delete_schedule_sql_response.log_print()


def update_schedule_sql():
    scheduled_sql_config = get_schedule_sql()

    configuration = scheduled_sql_config['configuration']
    schedule = scheduled_sql_config['schedule']

    configuration['fromTimeExpr'] = '@m-3m'
    configuration['maxRetries'] = 25

    schedule['type'] = 'Cron'
    schedule['interval'] = ''
    schedule['dayOfWeek'] = ''
    schedule['hour'] = ''
    schedule['cronExpression'] = '*/2 * * * *'

    schedule_sql = ScheduledSQL()
    schedule_sql.setSchedule(schedule)
    schedule_sql.setConfiguration(configuration)
    schedule_sql.setName(job_name)
    schedule_sql.setDisplayName(display_name)
    schedule_sql.setDescription(description)

    update_schedule_sql_response = client.update_scheduled_sql(project, schedule_sql)
    update_schedule_sql_response.log_print()


if __name__ == "__main__":
    create_schedule_sql()
    pass
