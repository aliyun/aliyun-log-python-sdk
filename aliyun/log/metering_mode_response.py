#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

from .util import Util
from .logresponse import LogResponse


class GetLogStoreMeteringModeResponse(LogResponse):
    """ The response of the get_logstore_metering_mode API from log.

    :type header: dict
    :param header: GetLogStoreMeteringModeResponse HTTP response header

    :type resp: dict
    :param resp: the HTTP response body
    """

    def __init__(self, resp, header):
        LogResponse.__init__(self, header, resp)
        self.metering_mode = Util.convert_unicode_to_str(resp["meteringMode"])

    def get_metering_mode(self):
        """

        :return: string, the metering mode of the logstore
        """
        return self.metering_mode

    def log_print(self):
        print('GetLogStoreMeteringModeResponse')
        print('headers:', self.get_all_headers())
        print('meteringMode:', self.metering_mode)

class GetMetricStoreMeteringModeResponse(LogResponse):
    """ The response of the get_metric_store_metering_mode API from log.

    :type header: dict
    :param header: GetMetricStoreMeteringModeResponse HTTP response header

    :type resp: dict
    :param resp: the HTTP response body
    """

    def __init__(self, resp, header):
        LogResponse.__init__(self, header, resp)
        self.metering_mode = Util.convert_unicode_to_str(resp["meteringMode"])

    def get_metering_mode(self):
        """
        :return: string, the metering mode of the metric store
        """
        return self.metering_mode

    def log_print(self):
        print('GetMetricStoreMeteringModeResponse')
        print('headers:', self.get_all_headers())
        print('meteringMode:', self.metering_mode)
        
class UpdateLogStoreMeteringModeResponse(LogResponse):
    """ The response of the update_logstore_metering_mode API from log.

    :type header: dict
    :param header: UpdateLogStoreMeteringModeResponse HTTP response header
    """

    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('UpdateLogStoreMeteringModeResponse:')
        print('headers:', self.get_all_headers())
        
class UpdateMetricStoreMeteringModeResponse(LogResponse):
    """ The response of the update_metric_store_metering_mode API from log.

    :type header: dict
    :param header: UpdateMetricStoreMeteringModeResponse HTTP response header
    """

    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('UpdateMetricStoreMeteringModeResponse:')
        print('headers:', self.get_all_headers())