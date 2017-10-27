#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.


class LogException(Exception):
    """The Exception of the log request & response.
    
    :type errorCode: string
    :param errorCode: log service error code 
    
    :type errorMessage: string
    :param errorMessage: detailed information for the exception
    
    :type requestId: string
    :param requestId: the request id of the response, '' is set if client error
    """

    def __init__(self, errorCode, errorMessage, requestId=''):
        self._errorCode = errorCode
        self._errorMessage = errorMessage
        self._requestId = requestId

    def __str__(self):
        return "LogException: \n{\n    ErrorCode: %s\n    ErrorMessage: %s\n    RequestId: %s\n}\n" \
               % (self._errorCode, self._errorMessage, self._requestId)

    def get_error_code(self):
        """ return error code of exception
        
        :return: string, error code of exception.
        """
        return self._errorCode

    def get_error_message(self):
        """ return error message of exception
        
        :return: string, error message of exception.
        """
        return self._errorMessage

    def get_request_id(self):
        """ return request id of exception. if client exception, request id is empty string
        
        :return: string, request id of exception.
        """
        return self._requestId
