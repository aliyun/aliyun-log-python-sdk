from .logresponse import LogResponse

__all__ = ['CreateRebuildIndexResponse', 'GetRebuildIndexResponse']


class CreateRebuildIndexResponse(LogResponse):
    """ Response of create_rebuild_index

    :type resp: dict
    :param resp: CreateRebuildIndexResponse HTTP response body

    :type header: dict
    :param header: CreateRebuildIndexResponse HTTP response header
    """

    def __init__(self, header, resp):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('CreateRebuildIndexResponse:')
        print('headers:', self.get_all_headers())


class GetRebuildIndexResponse(LogResponse):
    """ The response of the get_rebuild_index.

    :type resp: dict
    :param resp: GetRebuildIndexResponse HTTP response body

    :type header: dict
    :param header: GetRebuildIndexResponse HTTP response header
    """

    def __init__(self, header, resp):
        LogResponse.__init__(self, header, resp)
        self._status = resp['status']
        self._execution_details = resp.get('executionDetails')
        self._configuration = resp['configuration']

    def get_status(self):
        """job status
        """
        return self._status

    def get_execution_details(self):
        """job execution details
        """
        return self._execution_details

    def get_configuration(self):
        """job configuration
        """
        return self._configuration

    def log_print(self):
        print('GetRebuildIndexResponse:')
        print('headers:', self.get_all_headers())
        print('status: ', self.get_status())
        print('execution_details: ', self.get_execution_details())
        print('configuration: ', self.get_configuration())
