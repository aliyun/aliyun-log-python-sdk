# -*- coding: utf-8 -*-

from aliyun.log.logrequest import LogRequest
import json
import six

__all__ = ['CreateConsumerGroupRequest', 'ConsumerGroupGetCheckPointRequest',
           'ConsumerGroupHeartBeatRequest', 'ConsumerGroupUpdateCheckPointRequest']


class ConsumerGroupRequest(LogRequest):
    def __init__(self, project, logstore):
        LogRequest.__init__(self, project)
        # super(ConsumerGroupRequest, self).__init__(project)
        self.logstore = logstore

    def get_logstore(self):
        return self.logstore

    def set_logstore(self, logstore):
        self.logstore = logstore


class ConsumerGroupHeartBeatRequest(ConsumerGroupRequest):
    def __init__(self, project, logstore, consumer_group, consumer, shards):
        ConsumerGroupRequest.__init__(self, project, logstore)
        self.consumer_group = consumer_group
        self.consumer = consumer
        self.shards = shards

    def get_params(self):
        params = {
            'type': 'heartbeat',
            'consumer': self.consumer
        }
        return params

    def get_shards(self):
        return self.shards

    def set_shards(self, shards):
        self.shards = shards

    def get_request_body(self):
        return six.b(json.dumps(self.shards))


class ConsumerGroupGetCheckPointRequest(ConsumerGroupRequest):
    def __init__(self, project, logstore, consumer_group, shard):
        ConsumerGroupRequest.__init__(self, project, logstore)
        self.consumer_group = consumer_group
        self.shard = shard

    def get_params(self):
        if self.shard >= 0:
            return {'shard': self.shard}
        else:
            return {}

    def get_consumer_group(self):
        return self.consumer_group

    def set_consumer_group(self, consumer_group):
        self.consumer_group = consumer_group


class ConsumerGroupUpdateCheckPointRequest(ConsumerGroupRequest):
    def __init__(self, project, logstore, consumer_group, consumer, shard, check_point, force_success=True):
        ConsumerGroupRequest.__init__(self, project, logstore)
        self.consumer_group = consumer_group
        self.consumer = consumer
        self.shard = shard
        self.check_point = check_point
        self.force_success = force_success

    def get_consumer_group(self):
        return self.consumer_group

    def set_consumer_group(self, consumer_group):
        self.consumer_group = consumer_group

    def get_request_body(self):
        body_dict = {
            'shard': int(self.shard),
            'checkpoint': self.check_point
        }
        return six.b(json.dumps(body_dict))

    def get_request_params(self):
        params = {
            'type': 'checkpoint',
            'consumer': self.consumer,
            'forceSuccess': self.force_success
        }
        return params


class CreateConsumerGroupRequest(ConsumerGroupRequest):
    def __init__(self, project, logstore, consumer_group):
        ConsumerGroupRequest.__init__(self, project, logstore)
        # super(CreateConsumerGroupRequest, self).__init__(project, logstore)
        self.consumer_group = consumer_group

    def get_consumer_group(self):
        return self.consumer_group

    def set_consuemr_group(self, consumer_group):
        self.consumer_group = consumer_group
