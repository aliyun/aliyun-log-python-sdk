# -×- coding: utf-8 -*-

import json
import six


class ConsumerGroupEntity(object):
    def __init__(self, consumer_group_name, timeout, in_order=False):
        self.consumer_group_name = consumer_group_name
        self.timeout = timeout
        self.in_order = in_order

    def get_consumer_group_name(self):
        return self.consumer_group_name

    def set_consumer_group_name(self, consumer_group_name):
        self.consumer_group_name = consumer_group_name

    def get_timeout(self):
        return self.timeout

    def set_timeout(self, timeout):
        self.timeout = timeout

    def is_in_order(self):
        return self.in_order

    def set_in_order(self, in_order):
        self.in_order = in_order

    def to_request_json(self):
        log_store_dict = {
            'consumerGroup': self.get_consumer_group_name(),
            'timeout': self.get_timeout(),
            'order': self.is_in_order()
        }
        return six.b(json.dumps(log_store_dict))

    def to_string(self):
        return "ConsumerGroup [consumerGroupName=" + self.consumer_group_name \
               + ", timeout=" + str(self.timeout) + ", inOrder=" + str(self.in_order) + "]"
