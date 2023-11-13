#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.


class LogRequest(object):
    """ The base request of all log request.

    :type project: string
    :param project: project name
    """

    def __init__(self, project):
        self.project = project

    def get_project(self):
        """ Get project name

        :return: string, project name.
        """
        return self.project if self.project else ''

    def set_project(self, project):
        """ Set project name

        :type project: string
        :param project: project name
        """
        self.project = project
