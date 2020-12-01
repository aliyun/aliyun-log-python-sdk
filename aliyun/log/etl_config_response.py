from .logresponse import LogResponse
from .util import Util

__all__ = ['CreateEtlResponse', 'DeleteEtlResponse', 'GetEtlResponse', 'UpdateEtlResponse',
           'ListEtlsResponse', 'StopEtlResponse', 'StartEtlResponse']


class CreateEtlResponse(LogResponse):
    """ The response of the create_etl API from log.
    :type resp: dict
    :param resp: CreateEtlResponse HTTP response body
    :type header: dict
    :param header: CreateEtlResponse HTTP response header
    """

    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('CreateEtlResponse:')
        print('headers:', self.get_all_headers())


class DeleteEtlResponse(LogResponse):
    """ The response of the delete_etl API from log.
    :type resp: dict
    :param resp: DeleteEtlResponse HTTP response body
    :type header: dict
    :param header: DeleteEtlResponse HTTP response header
    """

    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('DeleteEtlResponse:')
        print('headers:', self.get_all_headers())


class GetEtlResponse(LogResponse):
    """ The response of the get_etl API from log.
    :type resp: dict
    :param resp: GetEtlResponse HTTP response body
    :type header: dict
    :param header: GetEtlResponse HTTP response header
    """

    def __init__(self, header, resp):
        LogResponse.__init__(self, header, resp)
        self.etl_config = resp

    def get_etl(self):
        """Get etl from the response
        :return: dict, etl
        """
        return self.etl_config

    def log_print(self):
        print('GetEtlResponse:')
        print('headers:', self.get_all_headers())
        print('etl: ', self.etl_config)


class UpdateEtlResponse(LogResponse):
    """ The response of the update_etl API from log.
    :type resp: dict
    :param resp: UpdateEtlResponse HTTP response header
    :type header: dict
    :param header: UpdateEtlResponse HTTP response header
    """

    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('UpdateEtlResponse:')
        print('headers:', self.get_all_headers())


class StartEtlResponse(LogResponse):
    """ The response of the start_etl API from log.
    :type resp: dict
    :param resp: StartEtlResponse HTTP response body
    :type header: dict
    :param header: StartEtlResponse HTTP response header
    """

    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('StartEtlResponse:')
        print('headers:', self.get_all_headers())


class StopEtlResponse(LogResponse):
    """ The response of the stop_etl API from log.
    :type resp: dict
    :param resp: StopEtlResponse HTTP response body
    :type header: dict
    :param header: StopEtlResponse HTTP response header
    """

    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('StopEtlResponse:')
        print('headers:', self.get_all_headers())


class ListEtlsResponse(LogResponse):
    """ The response of the list_etls API from log.
    :type header: dict
    :param header: ListEtlsResponse HTTP response header
    :type resp: dict
    :param resp: ListEtlsResponse HTTP response body
    """

    def __init__(self, resp, header):
        LogResponse.__init__(self, header, resp)
        self.count = int(resp["count"])
        self.total = int(resp["total"])
        self.etls = Util.convert_unicode_to_str(resp.get("results", []))

    def get_etls(self):
        """Get all the etls from the response
        :return: list, all etls
        """
        return self.etls

    def get_count(self):
        """ Get count of etls from the response
        :return: int, the number of count etls from the response
        """
        return self.count

    def get_total(self):
        """Get total count of etls from the response
        :return: int, the number of total etls from the response
        """
        return self.total

    def log_print(self):
        print('ListEtlResponse:')
        print('headers:', self.get_all_headers())
        print('count:', str(self.count))
        print('total:', str(self.total))
        print('etls:', self.etls)
