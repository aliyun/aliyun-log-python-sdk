#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.


class Histogram(object):
    """
    The class used to present the result of log histogram status. For every log
    histogram, it contains : from/to time range, hit log count and query
    completed status.

    :type fromTime: int
    :param fromTime: the begin time

    :type toTime: int
    :param toTime: the end time

    :type count: int
    :param count: log count of histogram that query hits

    :type progress: string
    :param progress: histogram query status(Complete or InComplete)
    """

    def __init__(self, fromTime, toTime, count, progress):
        self.fromTime = fromTime
        self.toTime = toTime
        self.count = count
        self.progress = progress

    def get_from(self):
        """ Get begin time

        :return: int, begin time
        """
        return self.fromTime

    def get_to(self):
        """ Get end time

        :return: int, end time
        """
        return self.toTime

    def get_count(self):
        """ Get log count of histogram that query hits

        :return: int, log count of histogram that query hits
        """
        return self.count

    def is_completed(self):
        """ Check if the histogram is completed

        :return: bool, true if this histogram is completed
        """
        return self.progress == 'Complete'

    def log_print(self):
        print('Histogram:')
        print('from:', self.fromTime)
        print('to:', self.toTime)
        print('count:', self.count)
        print('progress:', self.progress)
