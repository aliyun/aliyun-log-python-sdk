#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

from .logrequest import LogRequest
from .util import parse_timestamp


class UpdateLogsRequest(LogRequest):
    """The request used to update logs in a logstore."""

    def __init__(self, project=None, logstore=None, fromTime=None, toTime=None,
                 query=None, rowId=None, updateMode=None, data=None):
        LogRequest.__init__(self, project)
        self.logstore = logstore
        self.fromTime = parse_timestamp(fromTime) if fromTime is not None else fromTime
        self.toTime = parse_timestamp(toTime) if toTime is not None else toTime
        self.query = query
        self.rowId = rowId
        self.updateMode = updateMode
        self.data = data

    def get_logstore(self):
        """Get logstore name.

        :return: string, logstore name.
        """
        return self.logstore if self.logstore else ''

    def set_logstore(self, logstore):
        """Set logstore name.

        :type logstore: string
        :param logstore: logstore name
        """
        self.logstore = logstore

    def get_from(self):
        """Get begin time.

        :return: int, begin time
        """
        return self.fromTime

    def set_from(self, fromTime):
        """Set begin time.

        :type fromTime: int/string
        :param fromTime: begin time
        """
        self.fromTime = parse_timestamp(fromTime) if fromTime is not None else fromTime

    def get_to(self):
        """Get end time.

        :return: int, end time
        """
        return self.toTime

    def set_to(self, toTime):
        """Set end time.

        :type toTime: int/string
        :param toTime: end time
        """
        self.toTime = parse_timestamp(toTime) if toTime is not None else toTime

    def get_query(self):
        """Get user defined query.

        :return: string, user defined query
        """
        return self.query

    def set_query(self, query):
        """Set user defined query.

        :type query: string
        :param query: user defined query
        """
        self.query = query

    def get_row_id(self):
        """Get row id.

        :return: string, row id
        """
        return self.rowId

    def set_row_id(self, rowId):
        """Set row id.

        :type rowId: string
        :param rowId: row id
        """
        self.rowId = rowId

    def get_update_mode(self):
        """Get update mode.

        :return: string, update mode
        """
        return self.updateMode

    def set_update_mode(self, updateMode):
        """Set update mode.

        :type updateMode: string
        :param updateMode: update mode
        """
        self.updateMode = updateMode

    def get_data(self):
        """Get update data.

        :return: string, update data
        """
        return self.data

    def set_data(self, data):
        """Set update data.

        :type data: string
        :param data: update data
        """
        self.data = data
