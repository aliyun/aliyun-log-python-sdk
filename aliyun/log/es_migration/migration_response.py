#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.


from ..logresponse import LogResponse


class MigrationResponse(LogResponse):
    def __init__(self, headers=None, resp=""):
        LogResponse.__init__(self, headers, resp)
