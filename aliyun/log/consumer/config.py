# -*- coding: utf-8 -*-

from enum import Enum


class CursorPosition(Enum):
    BEGIN_CURSOR = 'BEGIN_CURSOR'
    END_CURSOR = 'END_CURSOR'
    SPECIAL_TIMER_CURSOR = 'SPECIAL_TIMER_CURSOR'


class ConsumerStatus(Enum):
    INITIALIZING = 'INITIALIZING'
    PROCESSING = 'PROCESSING'
    SHUTTING_DOWN = 'SHUTTING_DOWN'
    SHUTDOWN_COMPLETE = 'SHUTDOWN_COMPLETE'


class LogHubConfig(object):

    def __init__(self, endpoint, access_key_id, access_key, project, logstore,
                 consumer_group_name, consumer_name,
                 cursor_position, heartbeat_interval=20, data_fetch_interval=2, in_order=False,
                 cursor_start_time=-1, security_token=None):
        self.endpoint = endpoint
        self.accessKeyId = access_key_id
        self.accessKey = access_key
        self.project = project
        self.logstore = logstore
        self.consumer_group_name = consumer_group_name
        self.consumer_name = consumer_name
        self.cursor_position = cursor_position
        self.heartbeat_interval = heartbeat_interval
        self.data_fetch_interval = data_fetch_interval
        self.in_order = in_order
        self.cursor_start_time = cursor_start_time
        self.securityToken = security_token
