from .logresponse import LogResponse

__all__ = ['CreateEntityResponse', 'UpdateEntityResponse', 'DeleteEntityResponse', 'GetEntityResponse',
           'ListEntityResponse']


class CreateEntityResponse(LogResponse):
    pass


class UpdateEntityResponse(LogResponse):
    pass


class DeleteEntityResponse(LogResponse):
    pass


class GetEntityResponse(LogResponse):
    def get_entity(self):
        """
        Get entity itself
        :return:
        """
        return self.body

    def log_print(self):
        print('header: ', self.headers)
        print('body: ', self.body)


class ListEntityResponse(LogResponse):
    def __init__(self, header, resp, resource_name=None, entities_key=None):
        LogResponse.__init__(self, header, resp)
        self.count = resp['count']
        self._total = resp['total']
        backup_resource_name = ''
        if not resource_name:
            for x in resp:
                if x not in ('count', 'total'):
                    if x.endswith('s'):
                        resource_name = x
                        break
                    else:
                        backup_resource_name = x
            else:
                resource_name = backup_resource_name

        self.resource_name = resource_name
        if entities_key is None:
            entities_key = resource_name
        self.entities = resp.get(entities_key, [])

        if self.resource_name:
            setattr(self, 'get_' + resource_name, self.get_entities)

    def get_entities(self):
        """
        Get entities
        :return:
        """
        return self.entities

    def get_count(self):
        """ Get total count of entities

        :return: int, the number of total entities from the response
        """
        return self.count

    def get_total(self):
        """
        Get total count
        :return:
        """
        return self._total

    @property
    def total(self):
        return self._total

    def log_print(self):
        print('ListResponse for {0}:'.format(self.resource_name))
        print('headers:', self.get_all_headers())
        print('count:', self.count)
        print('total:', self._total)
        print(self.resource_name, self.entities)

    def merge(self, response):
        if not isinstance(response, ListEntityResponse):
            raise ValueError("passed response is not a ListEntityResponse: " + str(type(response)))

        self.count += response.get_count()
        self._total = response.get_total()  # use the latest total count
        self.entities.extend(response.get_entities())

        # update body
        self.body = {
            'count': self.count,
            'total': self._total,
            self.resource_name: self.entities
        }

        return self
