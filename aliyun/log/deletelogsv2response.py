#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

from .logresponse import LogResponse


class DeleteLogsV2Response(LogResponse):
    """The response of the DeleteLogsV2 API from log."""

    def __init__(self, resp, header):
        LogResponse.__init__(self, header, resp)
        self.affected_rows = resp.get('affected_rows', 0)

    def get_affected_rows(self):
        return self.affected_rows

    def log_print(self):
        print('DeleteLogsV2Response:')
        print('headers:', self.get_all_headers())
        print('affected_rows:', self.affected_rows)
