#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

from .logrequest import LogRequest


class DeleteAsyncSqlRequest(LogRequest):
    """ The request used to delete async SQL query.

    :type project: string
    :param project: project name

    :type query_id: string
    :param query_id: async SQL query ID
    """

    def __init__(self, project=None, query_id=None):
        LogRequest.__init__(self, project)
        self.query_id = query_id

    def get_query_id(self):
        """ Get query ID

        :return: string, query ID
        """
        return self.query_id if self.query_id else ''

    def set_query_id(self, query_id):
        """ Set query ID

        :type query_id: string
        :param query_id: query ID
        """
        self.query_id = query_id
