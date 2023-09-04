#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.
from .logresponse import LogResponse
from enum import Enum

class MeteringMode(Enum):
    """ metering mode supported
    """
    ChargeByDataIngest = 'ChargeByDataIngest'
    ChargeByFunction = 'ChargeByFunction'
    def __str__(self):
        return self.name

    @staticmethod
    def from_string(string):
        try:
            return MeteringMode[string]
        except KeyError:
            raise ValueError(string + " is not a valid MeteringMode")

class GetLogstoreMeteringModeResponse(LogResponse):
    """ The response of the get_logstore_metetring_mode API.
    
    :type header: dict
    :param header: GetLogstoreMeteringModeResponse HTTP response header
    """
    def __init__(self, resp, header):
        LogResponse.__init__(self, headers=header, body=resp)
        self.metering_mode = resp.get('meteringMode')
    
    def get_metering_mode(self):
        """ metering mode
        :type MeteringMode
        """
        return MeteringMode.from_string(self.metering_mode)

    def log_print(self):
            print('GetLogstoreMeteringModeResponse:')
            print('meteringMode:' + self.metering_mode)
            print('headers:', self.get_all_headers())

class UpdateLogstoreMeteringModeResponse(LogResponse):
    """ The response of the update_logstore_metering_mode API.
    
    :type header: dict
    :param header: UpdateLogstoreMeteringModeResponse HTTP response header
    """

    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('UpdateLogStoreMeteringModeResponse:')
        print('headers:', self.get_all_headers())
    