# -*- coding: utf-8 -*-


from .entity import ConsumerGroupEntity
from ..logresponse import LogResponse
from ..logexception import LogException


class CreateConsumerGroupResponse(LogResponse):
    def __init__(self, headers, resp=''):
        LogResponse.__init__(self, headers, resp)


class ConsumerGroupCheckPointResponse(LogResponse):
    def __init__(self, resp, headers):
        LogResponse.__init__(self, headers, resp)
        self.count = len(resp)
        self.consumer_group_check_poins = resp

    def get_count(self):
        return self.count

    def get_consumer_group_check_points(self):
        return self.consumer_group_check_poins

    def log_print(self):
        print('ListConsumerGroupCheckPoints:')
        print('headers:', self.get_all_headers())
        print('count:', self.count)
        print('consumer_group_check_points:', self.consumer_group_check_poins)

    def check_checkpoint(self, client, project_name, logstore_name):
        for checkpoint in self.consumer_group_check_poins:
            cursor = checkpoint["checkpoint"]
            shard_id = checkpoint["shard"]
            ret = client.get_previous_cursor_time(project_name, logstore_name, shard_id, cursor)
            checkpoint["checkpoint_previous_cursor_time"] = ret.get_cursor_time()


class ConsumerGroupHeartBeatResponse(LogResponse):
    def __init__(self, resp, headers):
        LogResponse.__init__(self, headers, resp)
        self.shards = resp

    def get_shards(self):
        return self.shards

    def set_shards(self, shards):
        self.shards = shards

    def log_print(self):
        print('ListHeartBeat:')
        print('headers:', self.get_all_headers())
        print('shards:', self.shards)


class ConsumerGroupUpdateCheckPointResponse(LogResponse):
    def __init__(self, headers, resp=''):
        LogResponse.__init__(self, headers, resp)


class DeleteConsumerGroupResponse(LogResponse):
    def __init__(self, headers, resp=''):
        LogResponse.__init__(self, headers, resp)


class ListConsumerGroupResponse(LogResponse):
    def __init__(self, resp, headers):
        LogResponse.__init__(self, headers, resp)
        self.count = len(resp)
        self.resp = resp
        self.consumer_groups = [ConsumerGroupEntity(group['name'], group['timeout'], group['order']) for group in
                                self.resp]

    def get_count(self):
        return self.count

    def get_consumer_groups(self):
        return self.consumer_groups

    def log_print(self):
        print('ListConsumerGroupResponse:')
        print('headers:', self.get_all_headers())
        print('count:', self.count)
        print('consumer_groups:', self.resp)


class UpdateConsumerGroupResponse(LogResponse):
    def __init__(self, headers, resp):
        LogResponse.__init__(self, headers, resp)
