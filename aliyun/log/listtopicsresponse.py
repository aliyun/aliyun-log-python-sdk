#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

from .logresponse import LogResponse
from .util import Util


class ListTopicsResponse(LogResponse):
    """ The response of the ListTopic API from log.

    :type resp: dict
    :param resp: ListTopicsResponse HTTP response body

    :type header: dict
    :param header: ListTopicsResponse HTTP response header
    """

    def __init__(self, resp, header):
        LogResponse.__init__(self, header, resp)
        self.count = Util.h_v_t(header, 'x-log-count')
        self.nextToken = Util.h_v_t(header, 'x-log-nexttoken')
        self.topics = resp

    def get_count(self):
        """ Get the number of all the topics from the response

        :return: int, the number of all the topics from the response
        """
        return self.count

    def get_topics(self):
        """ Get all the topics from the response

        :return: list, topic list
        """
        return self.topics

    def get_next_token(self):
        """ Return the next token from the response. If there is no more topic
        to list, it will return None

        :return: string, next token used to list more topics
        """
        return self.nextToken

    def log_print(self):
        print('ListTopicsResponse:')
        print('headers:', self.get_all_headers())
        print('count:', self.count)
        print('nextToken:', self.nextToken)
        print('topics:', self.topics)
