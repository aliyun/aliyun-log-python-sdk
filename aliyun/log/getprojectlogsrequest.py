#!/usr/bin/env python
#encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

from logrequest import LogRequest

class GetProjectLogsRequest(LogRequest):
    """ The request used to get logs by a query from log.
    
    :type project: string
    :param project: project name
    
    :type query: string
    :param query: user defined query
    """
    
    def __init__(self, project=None, query=None):
        LogRequest.__init__(self, project)
        self.query = query
    
    
   
    
    def get_query(self):
        """ Get user defined query
        
        :return: string, user defined query
        """
        return self.query
    
    
    def set_query(self, query):
        """ Set user defined query
        
        :type query: string
        :param query: user defined query
        """
        self.query = query



