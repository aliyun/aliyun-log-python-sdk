# encoding: utf-8
import json
import os
import time

from aliyun.log import *

# basic settings
endpoint = os.environ.get('ALIYUN_LOG_SAMPLE_ENDPOINT', '')
access_key_id = os.environ.get('ALIYUN_LOG_SAMPLE_ACCESSID', '')
access_key_secret = os.environ.get('ALIYUN_LOG_SAMPLE_ACCESSKEY', '')
project = os.environ.get('ALIYUN_LOG_SAMPLE_PROJECT', '')
logstore = os.environ.get('ALIYUN_LOG_SAMPLE_LOGSTORE', '')

client = LogClient(endpoint, access_key_id, access_key_secret)


def get_sample_job_config(job_name):
    job = {
        "name": job_name,
        "type": "Ingestion",
        "displayName": job_name,
        "state": "Enabled",
        "schedule": {
            "type": "Resident",
        },
        "configuration": {
            "version": "v2.0",
            "logstore": logstore,
            "source": {
                "type": "ElasticSearch",
                "BootstrapServers": "http://121.40.xx.xx:9200",  # 服务实例URL
                "Index": "index_1,index_2",                      # ES索引列表
                "Username": "elastic",                           # ES用户名称
                "Password": "my_password",                       # ES用户密码
                "TimeFieldName": "timestamp",                    # 时间字段（可选）
                "TimeFormat": "yyyy-MM-dd HH:mm:ss",             # 时间字段格式（可选），如epoch,yyyy-MM-dd HH:mm:ss等
                "TimeZone": "GMT+08:00",                         # 时间字段时区（可选）
                "Query": "gender:male and city:Shanghai",        # ES查询（可选）
                "ConnectorMode": "historical",                   # 导入模式，取值：incremental,historical
                "StartTime": "1682265600",                       # 起始时间（可选）
                "EndTime": "1682322749",                         # 结束时间（可选）
                "MaxDataDelaySec": "60",                         # 数据最大延迟秒数（增量导入模式时提供）
                "MinFragRangeSec": "60",                         # 检查新数据周期(秒)（增量导入模式时提供）
                "VpcId": "vpc-abcdexxxx",                        # VPC实例ID（可选，访问阿里云ES集群时可提供）
            }
        }
    }

    return json.dumps(job)


def create_job(job_name):
    if not job_name:
        job_name = "ingest-es-%d-0424" % int(time.time())

    job_config = get_sample_job_config(job_name)
    print("job config: ", job_config)

    client.create_ingestion(project_name=project, ingestion_config=job_config)


def restart_job(job_name):
    job_config = get_sample_job_config(job_name)
    print("job config: ", job_config)

    # update the job and then restart it
    client.restart_ingestion(project_name=project,
                             ingestion_name=job_name, ingestion_config=job_config)


def delete_job(job_name):
    client.delete_ingestion(project_name=project, ingestion_name=job_name)


if __name__ == '__main__':
    create_job(job_name="")
    # restart_job(job_name="ingest-es-1682322708-0424")
    # delete_job(job_name="ingest-es-1682322708-0424")
