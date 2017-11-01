# -*- coding: utf-8 -*-


from .entity import ConsumerGroupEntity
from ..logresponse import LogResponse


class CreateConsumerGroupResponse(LogResponse):
    def __init__(self, headers):
        LogResponse.__init__(self, headers)


class ConsumerGroupCheckPointResponse(LogResponse):
    def __init__(self, resp, headers):
        LogResponse.__init__(self, headers)
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


class ConsumerGroupHeartBeatResponse(LogResponse):
    def __init__(self, resp, headers):
        LogResponse.__init__(self, headers)
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
    def __init__(self, headers):
        LogResponse.__init__(self, headers)


class DeleteConsumerGroupResponse(LogResponse):
    def __init__(self, headers):
        LogResponse.__init__(self, headers)


class ListConsumerGroupResponse(LogResponse):
    def __init__(self, resp, headers):
        LogResponse.__init__(self, headers)
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
    def __init__(self, headers):
        LogResponse.__init__(self, headers)
