#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

from .logrequest import LogRequest


class ListDeleteLogsTasksRequest(LogRequest):
    """ The request used to list delete logs tasks from log.

    :type project: string
    :param project: project name

    :type logstore: string
    :param logstore: logstore name
    """

    def __init__(self, project=None, logstore=None):
        LogRequest.__init__(self, project)
        self.logstore = logstore

    def get_logstore(self):
        """ Get logstore name

        :return: string, logstore name.
        """
        return self.logstore if self.logstore else ''

    def set_logstore(self, logstore):
        """ Set logstore name

        :type logstore: string
        :param logstore: logstore name
        """
        self.logstore = logstore