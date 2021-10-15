from .logresponse import LogResponse
from .resource_params import *

from .util import Util

__all__ = ['CreateResourceResponse', 'DeleteResourceResponse', 'GetResourceResponse', 'UpdateResourceResponse',
           "ListResourcesResponse", 'CreateRecordResponse', 'DeleteRecordResponse', "UpdateRecordResponse",
           "UpsertRecordResponse", "GetRecordResponse", "ListRecordResponse"
           ]


class CreateResourceResponse(LogResponse):
    """ The response of the create_resource API from log.

    :type resp: dict
    :param resp: CreateResourceResponse HTTP response body

    :type header: dict
    :param header: CreateResourceResponse HTTP response header
    """

    def __init__(self, header, resp):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('CreateResourceResponse:')
        print('headers:', self.get_all_headers())


class DeleteResourceResponse(LogResponse):
    """ The response of the delete_resource API from log.

    :type resp: dict
    :param resp: DeleteResourceResponse HTTP response body

    :type header: dict
    :param header: DeleteResourceResponse HTTP response header
    """

    def __init__(self, header, resp):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('DeleteResourceResponse:')
        print('headers:', self.get_all_headers())


class ListResourcesResponse(LogResponse):
    """ The response of the list_resources API from log.

    :type header: dict
    :param header: ListResourcesResponse HTTP response header

    :type resp: dict
    :param resp: ListResourcesResponse HTTP response body
    """

    def __init__(self, resp, header):
        LogResponse.__init__(self, header, resp)
        self.count = int(resp["count"])
        self.total = int(resp["total"])
        self.resources = [Resource.from_dict(resource) for resource in Util.convert_unicode_to_str(resp.get("items", []))]

    def get_resources(self):
        """Get all the resources from the response

        :return: list, instances of Resource
        """
        return self.resources

    def get_count(self):
        """ Get count of resources from the response

        :return: int, the number of count resources from the response
        """
        return self.count

    def get_total(self):
        """Get total count of resources from the response

        :return: int, the number of total resources from the response
        """
        return self.total

    def log_print(self):
        print('ListResourcesResponse:')
        print('headers:', self.get_all_headers())
        print('count:', str(self.count))
        print('total:', str(self.total))
        print('resources:', self.resources)


class UpdateResourceResponse(LogResponse):
    """ The response of the update resource API from log.

    :type resp: dict
    :param resp: UpdateResourceResponse HTTP response body

    :type header: dict
    :param header: UpdateResourceResponse HTTP response header
    """

    def __init__(self, header, resp):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('UpdateResourceResponse:')
        print('headers:', self.get_all_headers())


class GetResourceResponse(LogResponse):
    """ The response of the get resource API from log.

    :type resp: dict
    :param resp: GetResourceResponse HTTP response body

    :type header: dict
    :param header: GetResourceResponse HTTP response header
    """

    def __init__(self, header, resp):
        LogResponse.__init__(self, header, resp)
        self.resource = Resource.from_dict(resp)

    def get_resource(self):
        """Get resource from the response

        :return: instance of Resource
        """
        return self.resource

    def log_print(self):
        print('GetResourceResponse:')
        print('headers:', self.get_all_headers())
        print('resource: ', self.resource)


class CreateRecordResponse(LogResponse):
    """ The response of the create record API from log.

        :type resp: dict
        :param resp: CreateRecordResponse HTTP response body

        :type header: dict
        :param header: CreateRecordResponse HTTP response header
        """

    def __init__(self, header, resp):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('CreateRecordResponse:')
        print('headers:', self.get_all_headers())


class DeleteRecordResponse(LogResponse):
    """ The response of the delete record API from log.

    :type resp: dict
    :param resp: DeleteRecordResponse HTTP response body

    :type header: dict
    :param header: DeleteRecordResponse HTTP response header
    """

    def __init__(self, header, resp):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('DeleteRecordResponse:')
        print('headers:', self.get_all_headers())


class UpdateRecordResponse(LogResponse):
    """ The response of the update record API from log.

        :type resp: dict
        :param resp: UpdateRecordResponse HTTP response body

        :type header: dict
        :param header: UpdateRecordResponse HTTP response header
        """

    def __init__(self, header, resp):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('UpdateRecordResponse:')
        print('headers:', self.get_all_headers())


class UpsertRecordResponse(LogResponse):
    """ The response of the update or insert record API from log.

        :type resp: dict
        :param resp: UpsertRecordResponse HTTP response body

        :type header: dict
        :param header: UpsertRecordResponse HTTP response header
        """

    def __init__(self, header, resp):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('UpsertRecordResponse:')
        print('headers:', self.get_all_headers())


class GetRecordResponse(LogResponse):
    """ The response of the get record API from log.

    :type resp: dict
    :param resp: GetRecordResponse HTTP response body

    :type header: dict
    :param header: GetRecordResponse HTTP response header
    """

    def __init__(self, header, resp):
        LogResponse.__init__(self, header, resp)
        self.record = ResourceRecord.from_dict(resp)

    def get_record(self):
        """Get record from the response

        :return: instance of ResourceRecord
        """
        return self.record

    def log_print(self):
        print('GetRecordResponse:')
        print('headers:', self.get_all_headers())
        print('record:', self.record)


class ListRecordResponse(LogResponse):
    """ The response of the list_records API from log.

    :type header: dict
    :param header: ListRecordResponse HTTP response header

    :type resp: dict
    :param resp: ListRecordResponse HTTP response body
    """

    def __init__(self, resp, header):
        LogResponse.__init__(self, header, resp)
        self.count = int(resp["count"])
        self.total = int(resp["total"])
        self.records = [ResourceRecord.from_dict(record) for record in Util.convert_unicode_to_str(resp.get("items", []))]

    def get_records(self):
        """Get all the records from the response

        :return: list, instances of ResourceRecord
        """
        return self.records

    def get_count(self):
        """ Get count of records from the response

        :return: int, the number of count records from the response
        """
        return self.count

    def get_total(self):
        """Get total count of records from the response

        :return: int, the number of total records from the response
        """
        return self.total

    def log_print(self):
        print('ListRecordsResponse:')
        print('headers:', self.get_all_headers())
        print('count:', str(self.count))
        print('total:', str(self.total))
        print('records:', self.records)
