#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.
import json

from .logresponse import LogResponse

__all__ = ['ListIngestionResponse', 'CreateIngestionResponse', 'UpdateIngestionResponse', 'DeleteIngestionResponse', 'GetIngestionResponse',
           'StartIngestionResponse', 'StopIngestionResponse', 'RestartIngestionResponse']


class ListIngestionResponse(LogResponse):
    """ The response of the ListIngestion API from log.

    :type resp: dict
    :param resp: ListIngestionResponse HTTP response body

    :type header: dict
    :param header: ListIngestionResponse HTTP response header
    """

    def __init__(self, resp, header):
        LogResponse.__init__(self, header, resp)
        self.count = resp['count']
        self.total = resp['total']
        self.ingestions = resp.get('results', [])

    def get_count(self):
        """ Get total count of ingestions from the response

        :return: int, the number of total ingestions from the response
        """
        return self.count

    def get_total(self):
        return self.total

    def get_ingestions(self):
        """ Get all the ingestions from the response

        :return: list, all ingestions
        """
        return self.ingestions

    def log_print(self):
        print('ListIngestionResponse:')
        print('headers:', self.get_all_headers())
        print('count:', self.count)
        print('total:', self.total)
        print('ingestions:', self.ingestions)


class CreateIngestionResponse(LogResponse):
    """ The response of the CreateIngestion API from log.

    :type resp: dict
    :param resp: CreateIngestionResponse HTTP response body

    :type header: dict
    :param header: CreateIngestionResponse HTTP response header
    """

    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('CreateIngestionResponse:')
        print('headers:', self.get_all_headers())


class UpdateIngestionResponse(LogResponse):
    """ The response of the UpdateIngestion API from log.

    :type resp: dict
    :param resp: UpdateIngestionResponse HTTP response body

    :type header: dict
    :param header: UpdateIngestionResponse HTTP response header
    """

    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('UpdateIngestionResponse:')
        print('headers:', self.get_all_headers())


class DeleteIngestionResponse(LogResponse):
    """ The response of the DeleteIngestion API from log.

    :type resp: dict
    :param resp: DeleteIngestionResponse HTTP response body

    :type header: dict
    :param header: DeleteIngestionResponse HTTP response header
    """

    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('DeleteIngestionResponse:')
        print('headers:', self.get_all_headers())


class GetIngestionResponse(LogResponse):
    """ The response of the GetIngestion API from log.

    :type resp: dict
    :param resp: GetIngestionResponse HTTP response body

    :type header: dict
    :param header: GetIngestionResponse HTTP response header
    """

    def __init__(self, resp, header):
        LogResponse.__init__(self, header, resp)
        self.ingestion_config = resp

    def get_ingestion(self):
        """ Get ingestion from the response

        :return: dict, ingestion
        """
        return self.ingestion_config

    def log_print(self):
        print('GetIngestionResponse:')
        print('headers:', self.get_all_headers())
        print('ingestion:', json.dumps(self.ingestion_config))


class StartIngestionResponse(LogResponse):
    """ The response of the StartIngestion API from log.

    :type resp: dict
    :param resp: StartIngestionResponse HTTP response body

    :type header: dict
    :param header: StartIngestionResponse HTTP response header
    """

    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('StartIngestionResponse:')
        print('headers:', self.get_all_headers())


class StopIngestionResponse(LogResponse):
    """ The response of the StopIngestion API from log.

    :type resp: dict
    :param resp: StopIngestionResponse HTTP response body

    :type header: dict
    :param header: StopIngestionResponse HTTP response header
    """

    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('StopIngestionResponse:')
        print('headers:', self.get_all_headers())

class RestartIngestionResponse(LogResponse):
    """ The response of the RestartIngestion API from log.

    :type resp: dict
    :param resp: RestartIngestionResponse HTTP response body

    :type header: dict
    :param header: RestartIngestionResponse HTTP response header
    """

    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('RestartIngestionResponse:')
        print('headers:', self.get_all_headers())
