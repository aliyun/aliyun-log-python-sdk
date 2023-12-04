#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

from .logrequest import LogRequest


class ListLogstoresRequest(LogRequest):
    """ The request used to list log store from log.

    :type project: string
    :param project: project name
    """

    def __init__(self, project=None):
        LogRequest.__init__(self, project)
