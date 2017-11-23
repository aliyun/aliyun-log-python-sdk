#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

from .logresponse import LogResponse


class ListLogstoresResponse(LogResponse):
    """ The response of the ListLogstores API from log.
    
    :type resp: dict
    :param resp: ListLogstoresResponse HTTP response body
    
    :type header: dict
    :param header: ListLogstoresResponse HTTP response header
    """

    def __init__(self, resp, header):
        LogResponse.__init__(self, header, resp)
        self.count = resp['count']
        self.total = resp['total']
        self.logstores = resp.get('logstores', [])

    def get_count(self):
        """ Get total count of logstores from the response
        
        :return: int, the number of total logstores from the response
        """
        return self.count

    def get_total(self):
        return self.total

    def get_logstores(self):
        """ Get all the logstores from the response
        
        :return: list, all logstores
        """
        return self.logstores

    def log_print(self):
        print('ListLogstoresResponse:')
        print('headers:', self.get_all_headers())
        print('count:', self.count)
        print('total:', self.total)
        print('logstores:', self.logstores)

    def merge(self, response):
        if not isinstance(response, ListLogstoresResponse):
            raise ValueError("passed response is not a ListLogstoresResponse: " + str(type(response)))

        self.count += response.get_count()
        self.total = response.get_total() # use the latest total count
        self.logstores.extend(response.get_logstores())

        # update body
        self.body = {
            'count': self.count,
            'total': self.total,
            'logstores': self.logstores
        }

        return self
