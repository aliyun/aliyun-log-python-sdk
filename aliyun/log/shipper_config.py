#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.


class OdpsShipperConfig(object):
    """ Odps shipper config

    :type odps_endpoint: string
    :param odps_endpoint: the odps endpoint

    :type odps_project: string
    :param odps_project: the odps project name

    :type odps_table: string
    :param odps_table: the odps table name

    :type log_fields_list: string array
    :param log_fields_list: the log field(keys in log) list mapping to the odps table column. e.g log_fields_list=['__time__', 'key_a', 'key_b'], the $log_time, $log_key_a, $log_key_b will mapping to odps table column No.1, No.2, No.3

    :type partition_column: string array
    :param partition_column: the log fields mapping to odps table partition column

    :type partition_time_format: string
    :param partition_time_format: the time format of __partition_time__, e.g yyyy_MM_dd_HH_mm

    """

    def __init__(self, odps_endpoint, odps_project, odps_table, log_fields_list, partition_column,
                 partition_time_format, bufferInterval=1800):
        self.odps_endpoint = odps_endpoint
        self.odps_project = odps_project
        self.odps_table = odps_table
        self.log_fields_list = log_fields_list
        self.partition_column = partition_column
        self.partition_time_format = partition_time_format
        self.buffer_interval = bufferInterval

    def to_json(self):
        json_value = {"odpsEndpoint": self.odps_endpoint, "odpsProject": self.odps_project,
                      "odpsTable": self.odps_table, "fields": self.log_fields_list,
                      "partitionColumn": self.partition_column, "partitionTimeFormat": self.partition_time_format,
                      "bufferInterval": self.buffer_interval}
        return json_value


class OssShipperConfig(object):
    """A oss ship config

    :type oss_bucket: string
    :param oss_bucket: the oss bucket name

    :type oss_prefix: string
    :param oss_prefix: the the prefix path where to save the log

    :type oss_role_arn: string
    :param oss_role_arn: the ram arn used to get the temporary write permission to the oss bucket

    :type buffer_interval: int
    :param buffer_interval: the time(seconds) to buffer before save to oss

    :type buffer_mb: int
    :param buffer_mb: the data size(MB) to buffer before save to oss

    :type compress_type: string
    :param compress_type: the compress type, only support 'snappy' or 'none'
    """

    def __init__(self, oss_bucket, oss_prefix, oss_role_arn, buffer_interval=300, buffer_mb=128,
                 compress_type='snappy'):
        self.oss_bucket = oss_bucket
        self.oss_prefix = oss_prefix
        self.oss_role_arn = oss_role_arn
        self.buffer_interval = buffer_interval
        self.buffer_mb = buffer_mb
        self.compress_type = compress_type

    def to_json(self):
        json_value = {'ossBucket': self.oss_bucket, 'ossPrefix': self.oss_prefix, 'roleArn': self.oss_role_arn,
                      'bufferInterval': self.buffer_interval, 'bufferSize': self.buffer_mb,
                      'compressType': self.compress_type}
        print(json_value)
        return json_value


class ShipperTask(object):
    """A shipper task

    :type task_id: string
    :param task_id: the task id

    :type task_status: string
    :param task_status: one of ['success', 'running', 'fail']

    :type task_message: string
    :param task_message: the error message of task_status is 'fail'

    :type task_create_time:  int
    :param task_create_time: the task create time (timestamp from 1970.1.1)

    :type task_last_data_receive_time: int
    :param task_last_data_receive_time: last log data receive time (timestamp)

    :type task_finish_time: int
    :param task_finish_time: the task finish time (timestamp)
    """

    def __init__(self, task_id, task_status, task_message, task_create_time, task_last_data_receive_time,
                 task_finish_time):
        self.task_id = task_id
        self.task_status = task_status
        self.task_message = task_message
        self.task_create_time = task_create_time
        self.task_last_data_receive_time = task_last_data_receive_time
        self.task_finish_time = task_finish_time

    def to_json(self):
        json_value = {'id': self.task_id, 'taskStatus': self.task_status, 'taskMessage': self.task_message,
                      'taskCreateTime': self.task_create_time,
                      'taskLastDataReceiveTime': self.task_last_data_receive_time,
                      'taskFinishTime': self.task_finish_time}
        return json_value
