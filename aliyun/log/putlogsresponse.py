#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

from .logresponse import LogResponse


class PutLogsResponse(LogResponse):
    """ The response of the PutLogs API from log.
    
    :type header: dict
    :param header: PutLogsResponse HTTP response header
    """

    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('PutLogsResponse:')
        print('headers:', self.get_all_headers())
