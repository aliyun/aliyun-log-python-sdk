# -*- coding: utf-8 -*-


from .logresponse import LogResponse
import six
import json

__all__ = ['ConsumerGroupEntity',
           'ConsumerGroupCheckPointResponse',
           'ConsumerGroupHeartBeatResponse',
           'ConsumerGroupUpdateCheckPointResponse',
           'CreateConsumerGroupResponse',
           'DeleteConsumerGroupResponse',
           'ListConsumerGroupResponse',
           'UpdateConsumerGroupResponse']


class ConsumerGroupEntity(object):
    def __init__(self, consumer_group_name, timeout, in_order=False):
        self.consumer_group_name = consumer_group_name
        self.timeout = timeout
        self.in_order = in_order

    def get_consumer_group_name(self):
        """

        :return:
        """
        return self.consumer_group_name

    def set_consumer_group_name(self, consumer_group_name):
        """

        :param consumer_group_name:
        :return:
        """
        self.consumer_group_name = consumer_group_name

    def get_timeout(self):
        """

        :return:
        """
        return self.timeout

    def set_timeout(self, timeout):
        """

        :param timeout:
        :return:
        """
        self.timeout = timeout

    def is_in_order(self):
        """

        :return:
        """
        return self.in_order

    def set_in_order(self, in_order):
        """

        :param in_order:
        :return:
        """
        self.in_order = in_order

    def to_request_json(self):
        """

        :return:
        """
        log_store_dict = {
            'consumerGroup': self.get_consumer_group_name(),
            'timeout': self.get_timeout(),
            'order': self.is_in_order()
        }
        return six.b(json.dumps(log_store_dict))

    def to_string(self):
        return "ConsumerGroup [consumerGroupName=" + self.consumer_group_name \
               + ", timeout=" + str(self.timeout) + ", inOrder=" + str(self.in_order) + "]"


class CreateConsumerGroupResponse(LogResponse):
    def __init__(self, headers, resp=''):
        LogResponse.__init__(self, headers, resp)


class ConsumerGroupCheckPointResponse(LogResponse):
    def __init__(self, resp, headers):
        LogResponse.__init__(self, headers, resp)
        self.count = len(resp)
        self.consumer_group_check_poins = resp

    def get_count(self):
        """

        :return:
        """
        return self.count

    def get_consumer_group_check_points(self):
        """

        :return:
        """
        return self.consumer_group_check_poins

    def log_print(self):
        """

        :return:
        """
        print('ListConsumerGroupCheckPoints:')
        print('headers:', self.get_all_headers())
        print('count:', self.count)
        print('consumer_group_check_points:', self.consumer_group_check_poins)

    def check_checkpoint(self, client, project_name, logstore_name):
        """

        :param client:
        :param project_name:
        :param logstore_name:
        :return:
        """
        for checkpoint in self.consumer_group_check_poins:
            cursor = checkpoint["checkpoint"]
            shard_id = checkpoint["shard"]
            if cursor:
                ret = client.get_previous_cursor_time(project_name, logstore_name, shard_id, cursor)
                checkpoint["checkpoint_previous_cursor_time"] = ret.get_cursor_time()


class ConsumerGroupHeartBeatResponse(LogResponse):
    def __init__(self, resp, headers):
        LogResponse.__init__(self, headers, resp)
        self.shards = resp

    def get_shards(self):
        """

        :return:
        """
        return self.shards

    def set_shards(self, shards):
        """

        :param shards:
        :return:
        """
        self.shards = shards

    def log_print(self):
        """

        :return:
        """
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
        """

        :return:
        """
        return self.count

    def get_consumer_groups(self):
        """

        :return:
        """
        return self.consumer_groups

    def log_print(self):
        """

        :return:
        """
        print('ListConsumerGroupResponse:')
        print('headers:', self.get_all_headers())
        print('count:', self.count)
        print('consumer_groups:', self.resp)


class UpdateConsumerGroupResponse(LogResponse):
    def __init__(self, headers, resp):
        LogResponse.__init__(self, headers, resp)
