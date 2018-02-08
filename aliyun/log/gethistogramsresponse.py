#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

from .logresponse import LogResponse
from .histogram import Histogram
from .util import Util


class GetHistogramsResponse(LogResponse):
    """ The response of the GetHistograms API from log.

    :type resp: dict
    :param resp: GetHistogramsResponse HTTP response body
    
    :type header: dict
    :param header: GetHistogramsResponse HTTP response header
    """

    def __init__(self, resp, header):
        LogResponse.__init__(self, header, resp)
        self.progress = Util.h_v_t(header, 'x-log-progress')
        self.count = 0  # header['x-log-count']
        self.histograms = []

        for data in resp:
            status = Histogram(data['from'], data['to'], data['count'], data['progress'])
            self.histograms.append(status)
            self.count += data['count']

    def is_completed(self):
        """ Check if the histogram is completed
        
        :return: bool, true if this histogram is completed
        """
        return self.progress == 'Complete'

    def get_total_count(self):
        """ Get total logs' count that current query hits
        
        :return: int, total logs' count that current query hits
        """
        return self.count

    def get_histograms(self):
        """ Get histograms on the requested time range: [from, to)
        
        :return: Histogram list, histograms on the requested time range: [from, to)
        """
        return self.histograms

    def log_print(self):
        print('GetHistogramsResponse:')
        print('headers:', self.get_all_headers())
        print('progress:', self.progress)
        print('count:', self.count)
        print('\nhistograms class:\n')
        for data in self.histograms:
            data.log_print()
            print('\n')
