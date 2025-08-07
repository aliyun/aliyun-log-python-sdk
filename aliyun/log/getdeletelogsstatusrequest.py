#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

from .logrequest import LogRequest
from .util import parse_timestamp

class GetDeleteLogsStatusRequest(LogRequest):
    """ The request used to get histograms of a query from log.

    :type project: string
    :param project: project name

    :type logstore: string
    :param logstore: logstore name

    :type taskid: string
    :param taskid: delete query taskid

    """

    def __init__(self, project=None, logstore=None, taskid=None):
        LogRequest.__init__(self, project)
        self.logstore = logstore
        self.taskid = taskid
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

    def get_taskid(self):
        """ Get taskid name

        :return: string, taskid name.
        """
        return self.taskid if self.taskid else ''

    def set_taskid(self, taskid):
        """ Set taskid name

        :type taskid: string
        :param taskid: taskid name
        """
        self.taskid = taskid

