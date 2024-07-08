#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.


from .logresponse import LogResponse
from .util import Util

__all__ = ['CreateExportResponse', 'DeleteExportResponse', 'GetExportResponse', 'ListExportResponse', 'UpdateExportResponse']


class CreateExportResponse(LogResponse):
    """
    Response of create_export
    """
    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('CreateExportResponse:')
        print('headers:', self.get_all_headers())


class DeleteExportResponse(LogResponse):
    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('DeleteExportResponse:')
        print('headers:', self.get_all_headers())


class UpdateExportResponse(LogResponse):
    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('UpdateExportResponse:')
        print('headers:', self.get_all_headers())


class GetExportResponse(LogResponse):
    """ The response of the get_export API from log.

    :type resp: dict
    :param resp: GetExportResponse HTTP response body

    :type header: dict
    :param header: GetExportResponse HTTP response header
    """

    def __init__(self, header, resp):
        LogResponse.__init__(self, header, resp)
        self.export_config = resp

    def get_export(self):
        """Get export from the response

        :return: dict, export
        """
        return self.export_config

    def log_print(self):
        print('GetExportResponse:')
        print('headers:', self.get_all_headers())
        print('export: ', self.export_config)


class ListExportResponse(LogResponse):
    """ The response of the list_export API from log.

    :type header: dict
    :param header: ListExportsResponse HTTP response header

    :type resp: dict
    :param resp: ListExportsResponse HTTP response body
    """

    def __init__(self, resp, header):
        LogResponse.__init__(self, header, resp)
        self.count = int(resp["count"])
        self.total = int(resp["total"])
        self.exports = Util.convert_unicode_to_str(resp.get("results", []))

    def get_exports(self):
        """Get all the exports from the response

        :return: list, all exports
        """
        return self.exports

    def get_count(self):
        """ Get count of exports from the response

        :return: int, the number of count exports from the response
        """
        return self.count

    def get_total(self):
        """Get total count of exports from the response

        :return: int, the number of total exports from the response
        """
        return self.total

    def log_print(self):
        print('ListExportResponse:')
        print('headers:', self.get_all_headers())
        print('count:', str(self.count))
        print('total:', str(self.total))
        print('exports:', self.exports)
