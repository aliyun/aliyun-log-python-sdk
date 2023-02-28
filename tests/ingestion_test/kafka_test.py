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
vpc_id = os.environ.get('ALIYUN_KAFKA_VPC_ID', '')

client = LogClient(endpoint, access_key_id, access_key_secret)


class TimeFieldInfo(object):
    """ customized time field info """

    def __init__(self, time_field, time_format, time_zone,
                 time_pattern=None, default_time_source="kafka"):
        self.time_field = time_field
        self.time_format = time_format
        self.time_zone = time_zone
        self.time_pattern = time_pattern
        self.default_time_source = default_time_source


def create_job_definition(job_name, logstore, bootstrap_server_list, topic_list,
                          time_field_info=None, consumer_group=None, vpc_id=None):
    source = {
        "bootstrapServers": ','.join(bootstrap_server_list),
        "fromPosition": "LATEST",
        "topics": ','.join(topic_list),
        "type": "Kafka",
        "valueType": "JSON"
    }

    # consumer group MUST be explicitly created if you're using
    # aliyun kafka product.
    if consumer_group is not None:
        source['consumerGroup'] = consumer_group

    # for kafka cluster created using aliyun ECS or aliyun kafka product,
    # vpc info needs to be provoided.
    if vpc_id is not None:
        source['vpcId'] = vpc_id

    if time_field_info is not None:
        source['timeField'] = time_field_info.time_field
        source['timeFormat'] = time_field_info.time_format
        source['timeZone'] = time_field_info.time_zone
        if time_field_info.time_pattern is not None:
            source['timePattern'] = time_field_info.time_pattern
        source['defaultTimeSource'] = time_field_info.default_time_source

    job = {
        "name": job_name,
        "type": "Ingestion",
        "displayName": job_name,
        "state": "Enabled",
        "schedule": {
            "type": "Resident",
            "runImmediately": True
        },
        "configuration": {
            "version": "v2.0",
            "logstore": logstore,
            "source": source
        }
    }
    return job


def get_sample_job_config(job_name, time_field_info=None):
    job = create_job_definition(
        job_name=job_name,
        logstore='kafka-ingestion',
        bootstrap_server_list=['192.168.26.34:9092'],
        topic_list=['^test-.*', 'your-topic'],
        consumer_group="will-test",
        vpc_id=vpc_id,
        time_field_info=time_field_info
    )
    return json.dumps(job)


def create_job(job_name):
    if not job_name:
        job_name = "ingest-kafka-%d-0228" % int(time.time())

    job_config = get_sample_job_config(job_name)
    print("job config: ", job_config)

    client.create_ingestion(project_name=project, ingestion_config=job_config)


def restart_job(job_name):
    time_field_info = TimeFieldInfo(
        time_field="time",
        time_format="dd/MMM/yyyy:hh:mm:ss",
        time_zone="GMT+08:00"
    )
    job_config = get_sample_job_config(job_name, time_field_info)
    print("job config: ", job_config)

    # update the job and then restart it
    client.restart_ingestion(project_name=project,
                             ingestion_name=job_name, ingestion_config=job_config)


if __name__ == '__main__':
    create_job(job_name="")
    # restart_job(job_name="ingest-kafka-1677575759-0228")
