#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

from .logrequest import LogRequest


class GetAsyncSqlRequest(LogRequest):
    """ The request used to get async SQL query results.

    :type project: string
    :param project: project name

    :type query_id: string
    :param query_id: async SQL query ID

    :type offset: int
    :param offset: line offset of return logs

    :type line: int
    :param line: max line number of return logs
    """

    def __init__(self, project=None, query_id=None, offset=0, line=100):
        LogRequest.__init__(self, project)
        self.query_id = query_id
        self.offset = offset
        self.line = line

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

    def get_offset(self):
        """ Get line offset of return logs

        :return: int, line offset of return logs
        """
        return self.offset

    def set_offset(self, offset):
        """ Set line offset of return logs

        :type offset: int
        :param offset: line offset of return logs
        """
        self.offset = offset

    def get_line(self):
        """ Get max line number of return logs

        :return: int, max line number of return logs
        """
        return self.line

    def set_line(self, line):
        """ Set max line number of return logs

        :type line: int
        :param line: max line number of return logs
        """
        self.line = line
