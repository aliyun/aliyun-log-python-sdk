# encoding: utf-8
from __future__ import print_function

import json

from aliyun.log import *


def main():
    endpoint = 'cn-hangzhou.log.aliyuncs.com'
    access_key_id = ''
    access_key = ''
    project = ''

    client = LogClient(endpoint, access_key_id, access_key)

    logstore = 'test'  # 日志库名称
    sink_logstore = 'test2'  # 加工目标日志库名称
    name = "metric_agg_rules3"  # 加工任务名称
    display_name = "metric_agg_rules3"  # 加工任务显示名
    schedule = {
        'type': 'Resident'
    }
    # log store to metric store
    scheduled_sql = {
        "agg_rules": [
            {
                "advanced_query": {
                    "labels": {"aliuid": "aliuid", "logstore": "logstore", "project": "project"},
                    "metric_names": ["success", "fail", "total"],
                    "query": "* | select max(__time__) as time, COUNT_if(Status < 500) as success, count_if(Status >= 500) as fail, count(1) as total, InvokerUid as aliuid, Project as project, LogStore as logstore from log  group by InvokerUid, Project, LogStore limit 100000",
                    "time_name": "time",
                    "type": "sql"
                },
                "rule_name": "metric_agg_rules1",
                "schedule_control": {
                    "delay": 30,
                    "from_unixtime": 1610506297,
                    "granularity": 30,
                    "to_unixtime": -1
                }
            },
            {
                "advanced_query": {
                    "labels": {
                        "agent": "agent",
                        "method": "method"
                    },
                    "metric_names": ["ok", "not_ok"],
                    "query": "* | select max(__time__) as time, COUNT_if(Status \\u003c 300) as ok, count_if(Status \\u003e= 300) as not_ok, Method as method,UserAgent as agent from log  group by method, agent limit 100000",
                    "time_name": "time",
                    "type": "sql"
                },
                "rule_name": "testId2",
                "schedule_control": {
                    "delay": 30,
                    "from_unixtime": 1610506297,
                    "granularity": 30,
                    "to_unixtime": -1
                }
            }
        ]
    }
    # metric store to metric store
    scheduled_sql = {
        "agg_rules": [
            {
                "advanced_query": {
                    "labels": {},
                    "metric_names": ["total_count"],
                    "query": "* | SELECT promql_query(\'sum(sum_over_time(total[1m]))\') FROM  metrics limit 1000",
                    "time_name": "time",
                    "type": "promql"
                },
                "rule_name": 'metric_agg_rules3',
                "schedule_control": {
                    "delay": 60,
                    "from_unixtime": 1610433565,
                    "granularity": 60,
                    "to_unixtime": -1
                }
            }
        ]
    }
    configuration = {
        'logstore': logstore,
        'instanceType': 'Standard',
        'version': 0,
        'fromTime': 1610503245,
        'script': '',
        'accessKeyId': access_key_id,
        'accessKeySecret': access_key,
        'roleArn': '',
        'parameters': {
            'config.ml.scheduled_sql': json.dumps(scheduled_sql),
            'sls.config.job_mode': '{"type":"ml","source":"ScheduledSQL"}'
        },
        'sinks': [
            {
                'type': 'AliyunLOG',
                'name': 'sls-convert-metric',
                'endpoint': endpoint,
                'project': project,
                'logstore': sink_logstore,
                'accessKeyId': access_key_id,
                'accessKeySecret': access_key,
                'roleArn': ''
            }
        ]
    }
    # create
    r = client.create_etl(project, name, configuration,
                          schedule, display_name, "description")
    r.log_print()
    # get
    r = client.get_etl(project, name)
    r.log_print()
    # update
    r = client.update_etl(project, name, configuration,
                          schedule, display_name, "description")
    r.log_print()
    # delete
    r = client.delete_etl(project, name)
    r.log_print()
    # list
    r = client.list_etls(project, 0, 100)
    etl_list = r.etls
    metric_agg_rule_list = []
    for i in range(len(etl_list)):
        etl = etl_list[i]
        if 'configuration' in etl.keys() and 'parameters' in etl['configuration'].keys() \
                and 'config.ml.scheduled_sql' in etl['configuration']['parameters'].keys():
            metric_agg_rule_list.append(etl)
    print('metric_agg_rules:', metric_agg_rule_list)


if __name__ == '__main__':
    main()
