#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

import six
class QueriedLog(object):
    """ The QueriedLog is a log of the GetLogsResponse which obtained from the log.

    :type timestamp: int
    :param timestamp: log timestamp

    :type source: string
    :param source: log source

    :type contents: dict
    :param contents: log contents, content many key/value pair
    """

    def __init__(self, timestamp, source, contents):
        self.timestamp = int(timestamp)
        self.source = source
        self.contents = contents

    @staticmethod
    def _from_dict(data):
        """ Initalize from dict
        """
        source = data.get("__source__", "")
        time = data.get("__time__", '0')
        contents = {}
        for key in six.iterkeys(data):
            if key not in ['__time__', '__source__']:
                contents[key] = data[key]

        return QueriedLog(time, source, contents)

    def _to_dict(self):
        res = {
            '__time__': str(self.timestamp),
            '__source__': self.source,
        }
        res.update(self.contents)
        return res

    def get_time(self):
        """ Get log time

        :return: int, log time
        """
        return self.timestamp

    def get_source(self):
        """ Get log source

        :return: string, log source
        """
        return self.source

    def get_contents(self):
        """ Get log contents

        :return: dict, log contents
        """
        return self.contents

    def log_print(self):
        print('QueriedLog:')
        print('time:', self.get_time())
        print('source:', self.get_source())
        print('contents:', self.get_contents())
