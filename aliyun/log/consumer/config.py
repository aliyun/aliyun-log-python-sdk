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
                 cursor_start_time=-1, security_token=None, max_fetch_log_group_size=1000, worker_pool_size=2):
        """

        :param endpoint:
        :param access_key_id:
        :param access_key:
        :param project:
        :param logstore:
        :param consumer_group_name:
        :param consumer_name: suggest use format "{consumer_group_name}-{current_process_id}", give it different consumer name when you need to run this program in parallel
        :param cursor_position: This options is used for initialization, will be ignored once consumer group is created and each shard has beeen started to be consumed.
        :param heartbeat_interval: once a client doesn't report to server * heartbeat_interval * 2 interval, server will consider it's offline and re-assign its task to another consumer. thus  don't set the heatbeat interval too small when the network badwidth or performance of consumtion is not so good.
        :param data_fetch_interval: don't configure it too small (<1s)
        :param in_order: during consuption, when shard is splitted, if need to consume the newly splitted shard after its parent shard (read-only) is finished consumption or not. suggest keep it as False (don't care) until you have good reasion for it.
        :param cursor_start_time: Will be used when cursor_position when could be "begin", "end", "specific time format in ISO", it's log receiving time.
        :param security_token:
        :param max_fetch_log_group_size: fetch size in each request, normally use default. maximum is 1000, could be lower. the lower the size the memory efficiency might be better.
        :param worker_pool_size: suggest keep the default size (2), use multiple process instead, when you need to have more concurrent processing, launch this consumer for mulitple times and give them different consuer name in same consumer group
        """
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
        self.max_fetch_log_group_size = max_fetch_log_group_size
        self.worker_pool_size = worker_pool_size

