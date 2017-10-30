#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

from .logrequest import LogRequest


class ListTopicsRequest(LogRequest):
    """ The request used to get topics of a query from log. 
    
    :type project: string
    :param project: project name
    
    :type logstore: string
    :param logstore: logstore name
    
    :type token: string
    :param token: the start token to list topics
    
    :type line: int
    :param line: max topic counts to return
    """

    def __init__(self, project=None, logstore=None, token=None, line=None):
        LogRequest.__init__(self, project)
        self.logstore = logstore
        self.token = token
        self.line = line

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

    def get_token(self):
        """ Get start token to list topics
        
        :return: string, start token to list topics
        """
        return self.token

    def set_token(self, token):
        """ Set start token to list topics
        
        :type token: string
        :param token: start token to list topics
        """
        self.token = token

    def get_line(self):
        """ Get max topic counts to return
        
        :return: int, max topic counts to return
        """
        return self.line

    def set_line(self, line):
        """ Set max topic counts to return
        
        :type line: int
        :param line: max topic counts to return
        """
        self.line = line
