from .logresponse import LogResponse
from .topostore_params import *

from .util import Util

__all__ = ['CreateTopostoreResponse', 'DeleteTopostoreResponse',
         'GetTopostoreResponse', 'UpdateTopostoreResponse',"ListTopostoresResponse",
        'CreateTopostoreNodeResponse', 'DeleteTopostoreNodeResponse',
        'GetTopostoreNodeResponse','UpsertTopostoreNodeResponse', 'UpdateTopostoreNodeResponse',"ListTopostoreNodesResponse",
        'CreateTopostoreRelationResponse', 'DeleteTopostoreRelationResponse',
         'GetTopostoreRelationResponse','UpsertTopostoreRelationResponse', 'UpdateTopostoreRelationResponse',"ListTopostoreRelationsResponse",
]

## Topostore

class CreateTopostoreResponse(LogResponse):
    """ The response of the CreateTopostore API from log.

    :type resp: dict
    :param resp: CreateTopostoreResponse HTTP response body

    :type header: dict
    :param header: CreateTopostoreResponse HTTP response header
    """

    def __init__(self, header, resp):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('CreateTopostoreResponse:')
        print('headers:', self.get_all_headers())


class DeleteTopostoreResponse(LogResponse):
    """ The response of the DeleteTopostore API from log.

    :type resp: dict
    :param resp: DeleteTopostoreResponse HTTP response body

    :type header: dict
    :param header: DeleteTopostoreResponse HTTP response header
    """

    def __init__(self, header, resp):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('DeleteTopostoreResponse:')
        print('headers:', self.get_all_headers())


class ListTopostoresResponse(LogResponse):
    """ The response of the ListTopostores API from log.

    :type header: dict
    :param header: ListTopostoresResponse HTTP response header

    :type resp: dict
    :param resp: ListTopostoresResponse HTTP response body
    """

    def __init__(self, resp, header):
        LogResponse.__init__(self, header, resp)
        self.count = int(resp["count"])
        self.total = int(resp["total"])
        self.topostores = [Topostore.from_dict(topostore) for topostore in Util.convert_unicode_to_str(resp.get("items", []))]

    def get_topostores(self):
        """Get all the topostores from the response

        :return: list, instances of Topostore
        """
        return self.topostores

    def get_count(self):
        """ Get count of topostores from the response

        :return: int, the number of count topostores from the response
        """
        return self.count

    def get_total(self):
        """Get total count of topostores from the response

        :return: int, the number of total Topostores from the response
        """
        return self.total

    def log_print(self):
        print('ListTopostoresResponse:')
        print('headers:', self.get_all_headers())
        print('count:', str(self.count))
        print('total:', str(self.total))
        print('topostores:', self.topostores)


class UpdateTopostoreResponse(LogResponse):
    """ The response of the UpdateTopostore API from log.

    :type resp: dict
    :param resp: UpdateTopostoreResponse HTTP response body

    :type header: dict
    :param header: UpdateTopostoreResponse HTTP response header
    """

    def __init__(self, header, resp):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('UpdateTopostoreResponse:')
        print('headers:', self.get_all_headers())


class GetTopostoreResponse(LogResponse):
    """ The response of the GetTopostore API from log.

    :type resp: dict
    :param resp: GetTopostoreResponse HTTP response body

    :type header: dict
    :param header: GetTopostoreResponse HTTP response header
    """

    def __init__(self, header, resp):
        LogResponse.__init__(self, header, resp)
        self.topostore = Topostore.from_dict(resp)

    def get_topostore(self):
        """Get Topostore from the response

        :return: instance of Topostore
        """
        return self.topostore

    def log_print(self):
        print('GetTopostoreResponse:')
        print('headers:', self.get_all_headers())
        print('topostore: ', self.topostore)



class CreateTopostoreResponse(LogResponse):
    """ The response of the CreateTopostore API from log.

    :type resp: dict
    :param resp: CreateTopostoreResponse HTTP response body

    :type header: dict
    :param header: CreateTopostoreResponse HTTP response header
    """

    def __init__(self, header, resp):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('CreateTopostoreResponse:')
        print('headers:', self.get_all_headers())


class DeleteTopostoreResponse(LogResponse):
    """ The response of the DeleteTopostore API from log.

    :type resp: dict
    :param resp: DeleteTopostoreResponse HTTP response body

    :type header: dict
    :param header: DeleteTopostoreResponse HTTP response header
    """

    def __init__(self, header, resp):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('DeleteTopostoreResponse:')
        print('headers:', self.get_all_headers())



## Node

class CreateTopostoreNodeResponse(LogResponse):
    """ The response of the CreateTopostoreNode API from log.

    :type resp: dict
    :param resp: CreateTopostoreNodeResponse HTTP response body

    :type header: dict
    :param header: CreateTopostoreNodeResponse HTTP response header
    """

    def __init__(self, header, resp):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('CreateTopostoreNodeResponse:')
        print('headers:', self.get_all_headers())

class UpsertTopostoreNodeResponse(LogResponse):
    """ The response of the UpsertTopostoreNode API from log.

    :type resp: dict
    :param resp: UpsertTopostoreNodeResponse HTTP response body

    :type header: dict
    :param header: UpsertTopostoreNodeResponse HTTP response header
    """

    def __init__(self, header, resp):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('UpsertTopostoreNodeResponse:')
        print('headers:', self.get_all_headers())


class DeleteTopostoreNodeResponse(LogResponse):
    """ The response of the DeleteTopostoreNode API from log.

    :type resp: dict
    :param resp: DeleteTopostoreNodeResponse HTTP response body

    :type header: dict
    :param header: DeleteTopostoreNodeResponse HTTP response header
    """

    def __init__(self, header, resp):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('DeleteTopostoreNodeResponse:')
        print('headers:', self.get_all_headers())


class ListTopostoreNodesResponse(LogResponse):
    """ The response of the ListTopostoreNodes API from log.

    :type header: dict
    :param header: ListTopostoreNodesResponse HTTP response header

    :type resp: dict
    :param resp: ListTopostoreNodesResponse HTTP response body
    """

    def __init__(self, resp, header):
        LogResponse.__init__(self, header, resp)
        self.count = int(resp["count"])
        self.total = int(resp["total"])
        self.nodes = [TopostoreNode.from_dict(node) for node in Util.convert_unicode_to_str(resp.get("items", []))]

    def get_nodes(self):
        """Get all the nodes from the response

        :return: list, instances of Node
        """
        return self.nodes

    def get_count(self):
        """ Get count of nodes from the response

        :return: int, the number of count Nodes from the response
        """
        return self.count

    def get_total(self):
        """Get total count of Nodes from the response

        :return: int, the number of total Nodes from the response
        """
        return self.total

    def log_print(self):
        print('ListTopostoreNodesResponse:')
        print('headers:', self.get_all_headers())
        print('count:', str(self.count))
        print('total:', str(self.total))
        print('nodes:', self.nodes)


class UpdateTopostoreNodeResponse(LogResponse):
    """ The response of the UpdateTopostoreNode API from log.

    :type resp: dict
    :param resp: UpdateTopostoreNodeResponse HTTP response body

    :type header: dict
    :param header: UpdateTopostoreNodeResponse HTTP response header
    """

    def __init__(self, header, resp):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('UpdateTopostoreNodeResponse:')
        print('headers:', self.get_all_headers())


class GetTopostoreNodeResponse(LogResponse):
    """ The response of the GetTopostoreNode API from log.

    :type resp: dict
    :param resp: GetTopostoreNodeResponse HTTP response body

    :type header: dict
    :param header: GetTopostoreNodeResponse HTTP response header
    """

    def __init__(self, header, resp):
        LogResponse.__init__(self, header, resp)
        self.node = TopostoreNode.from_dict(resp)

    def get_node(self):
        """Get Node from the response

        :return: instance of Node
        """
        return self.node

    def log_print(self):
        print('GetTopostoreNodeResponse:')
        print('headers:', self.get_all_headers())
        print('node: ', self.node.to_dict())


## relation

class CreateTopostoreRelationResponse(LogResponse):
    """ The response of the CreateTopostoreRelation API from log.

    :type resp: dict
    :param resp: CreateTopostoreRelationResponse HTTP response body

    :type header: dict
    :param header: CreateTopostoreRelationResponse HTTP response header
    """

    def __init__(self, header, resp):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('CreateTopostoreRelationResponse:')
        print('headers:', self.get_all_headers())


class UpsertTopostoreRelationResponse(LogResponse):
    """ The response of the UpsertTopostoreRelation API from log.

    :type resp: dict
    :param resp: UpsertTopostoreRelationResponse HTTP response body

    :type header: dict
    :param header: UpsertTopostoreRelationResponse HTTP response header
    """

    def __init__(self, header, resp):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('UpsertTopostoreRelationResponse:')
        print('headers:', self.get_all_headers())


class DeleteTopostoreRelationResponse(LogResponse):
    """ The response of the DeleteTopostoreRelation API from log.

    :type resp: dict
    :param resp: DeleteTopostoreRelationResponse HTTP response body

    :type header: dict
    :param header: DeleteTopostoreRelationResponse HTTP response header
    """

    def __init__(self, header, resp):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('DeleteTopostoreRelationResponse:')
        print('headers:', self.get_all_headers())


class ListTopostoreRelationsResponse(LogResponse):
    """ The response of the ListTopostoreRelations API from log.

    :type header: dict
    :param header: ListTopostoreRelationsResponse HTTP response header

    :type resp: dict
    :param resp: ListTopostoreRelationsResponse HTTP response body
    """

    def __init__(self, resp, header):
        LogResponse.__init__(self, header, resp)
        self.count = int(resp["count"])
        self.total = int(resp["total"])
        self.relations = [TopostoreRelation.from_dict(relation) for relation in Util.convert_unicode_to_str(resp.get("items", []))]

    def get_relations(self):
        """Get all the relations from the response

        :return: list, instances of Relation
        """
        return self.relations

    def get_count(self):
        """ Get count of relations from the response

        :return: int, the number of count Relations from the response
        """
        return self.count

    def get_total(self):
        """Get total count of Relations from the response

        :return: int, the number of total Relations from the response
        """
        return self.total

    def log_print(self):
        print('ListTopostoreRelationsResponse:')
        print('headers:', self.get_all_headers())
        print('count:', str(self.count))
        print('total:', str(self.total))
        print('relations:', self.relations)


class UpdateTopostoreRelationResponse(LogResponse):
    """ The response of the UpdateTopostoreRelation API from log.

    :type resp: dict
    :param resp: UpdateTopostoreRelationResponse HTTP response body

    :type header: dict
    :param header: UpdateTopostoreRelationResponse HTTP response header
    """

    def __init__(self, header, resp):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('UpdateTopostoreRelationResponse:')
        print('headers:', self.get_all_headers())


class GetTopostoreRelationResponse(LogResponse):
    """ The response of the GetTopostoreRelation API from log.

    :type resp: dict
    :param resp: GetTopostoreRelationResponse HTTP response body

    :type header: dict
    :param header: GetTopostoreRelationResponse HTTP response header
    """

    def __init__(self, header, resp):
        LogResponse.__init__(self, header, resp)
        self.relation = TopostoreRelation.from_dict(resp)

    def get_relation(self):
        """Get Relation from the response

        :return: instance of Relation
        """
        return self.relation

    def log_print(self):
        print('GetTopostoreRelationResponse:')
        print('headers:', self.get_all_headers())
        print('relation: ', self.relation)
