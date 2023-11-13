import six, json
from .logclient_operator import list_more
from .logexception import LogException
from .pluralize import pluralize
from .common_response import *

DEFAULT_MAX_LIST_PAGING_SIZE = 500


def create_entity(entity_name, logstore_level=None, root_resource=None):
    def do_create(self, project, detail, resource_prefix=None):
        params = {}
        headers = {"x-log-bodyrawsize": '0', "Content-Type": "application/json"}

        if hasattr(detail, 'to_json'):
            detail = detail.to_json()
            body_str = six.b(json.dumps(detail))
        elif isinstance(detail, six.binary_type):
            body_str = detail
        elif isinstance(detail, six.text_type):
            body_str = detail.encode('utf8')
        else:
            body_str = six.b(json.dumps(detail))

        resource_path = (root_resource and root_resource.rstrip('/')) or "/" + pluralize(entity_name)
        if resource_prefix:
            resource_path = resource_prefix + resource_path
        (resp, header) = self._send("POST", project, body_str, resource_path, params, headers)
        return GetEntityResponse(header, resp)

    if not logstore_level:
        def fn(self, project, detail):
            """ Create {entity_title}.
            Unsuccessful opertaion will cause an LogException.

            :type project: string
            :param project: project name

            :type detail: dict/string
            :param detail: json string

            :return: CreateEntityResponse

            :raise: LogException
            """
            return do_create(self, project, detail)
    else:
        def fn(self, project, logstore, detail):
            """ Create {entity_title}.
            Unsuccessful opertaion will cause an LogException.

            :type project: string
            :param project: project name

            :type logstore: string
            :param logstore: logstore name

            :type detail: dict/string
            :param detail: json string

            :return: CreateEntityResponse

            :raise: LogException
            """
            resource_prefix = "/logstores/" + logstore
            return do_create(self, project, detail, resource_prefix)

    fn.__name__ = 'create_' + entity_name
    fn.__doc__ = fn.__doc__.format(entity_title=entity_name.title())
    return fn


def get_entity(entity_name, logstore_level=None, root_resource=None):
    if not logstore_level:
        def fn(self, project, entity):
            """Get {entity_title}.
            Unsuccessful opertaion will cause an LogException.

            :type project: string
            :param project: project name

            :type entity: string
            :param entity: {entity_name} name

            :return: GetEntityResponse

            :raise: LogException
            """
            resource_path = ((root_resource and root_resource.rstrip('/')) or ('/' + pluralize(entity_name))) + '/' + entity

            (resp, header) = self._send("GET", project, None, resource_path, dict(), dict())
            return GetEntityResponse(header, resp)
    else:
        def fn(self, project, logstore, entity):
            """Get {entity_title}.
            Unsuccessful opertaion will cause an LogException.

            :type project: string
            :param project: project name

            :type logstore: string
            :param logstore: logstore name

            :type entity: string
            :param entity: {entity_name} name

            :return: GetEntityResponse

            :raise: LogException
            """
            resource_path = ((root_resource and root_resource.rstrip('/')) or ('/' + pluralize(entity_name))) + '/' + entity
            resource_path = "/logstores/" + logstore + resource_path

            (resp, header) = self._send("GET", project, None, resource_path, dict(), dict())
            return GetEntityResponse(header, resp)

    fn.__name__ = 'get_' + entity_name
    fn.__doc__ = fn.__doc__.format(entity_name=entity_name, entity_title=entity_name.title())

    return fn


def delete_entity(entity_name, logstore_level=None, root_resource=None):
    if not logstore_level:
        def fn(self, project, entity):
            """Delete {entity_title}.
            Unsuccessful opertaion will cause an LogException.

            :type project: string
            :param project: project name

            :type entity: string
            :param entity: {entity_name} name

            :return: DeleteEntityResponse

            :raise: LogException
            """
            resource_path = ((root_resource and root_resource.rstrip('/')) or ('/' + pluralize(entity_name))) + '/' + entity
            (resp, header) = self._send("DELETE", project, None, resource_path, dict(), dict())
            return DeleteEntityResponse(header, resp)
    else:
        def fn(self, project, logstore, entity):
            """Delete {entity_title}.
            Unsuccessful opertaion will cause an LogException.

            :type project: string
            :param project: project name

            :type logstore: string
            :param logstore: logstore name

            :type entity: string
            :param entity: {entity_name} name

            :return: DeleteEntityResponse

            :raise: LogException
            """
            resource_path = ((root_resource and root_resource.rstrip('/')) or ('/' + pluralize(entity_name))) + '/' + entity
            resource_path = "/logstores/" + logstore + resource_path
            (resp, header) = self._send("DELETE", project, None, resource_path, dict(), dict())
            return DeleteEntityResponse(header, resp)

    fn.__name__ = 'delete_' + entity_name
    fn.__doc__ = fn.__doc__.format(entity_name=entity_name, entity_title=entity_name.title())

    return fn


def list_entity(entity_name, logstore_level=None, root_resource=None, max_batch_size=DEFAULT_MAX_LIST_PAGING_SIZE, entities_key=None, raw_resource_name=None, job_type=None):
    def do_list(self, project, resource_path, offset=0, size=100):
        headers = {}
        params = {}
        resource_name = raw_resource_name if raw_resource_name else pluralize(entity_name)
        params['offset'] = str(offset)
        params['size'] = str(size)
        if root_resource == '/jobs' and job_type:
            params['jobType'] = str(job_type)
        (resp, header) = self._send("GET", project, None, resource_path, params, headers)
        return ListEntityResponse(header, resp, resource_name=resource_name, entities_key=entities_key)

    if not logstore_level:
        def fn(self, project, offset=0, size=100):
            """ list the {entity_title}, get first 100 items by default
            Unsuccessful opertaion will cause an LogException.

            :type project: string
            :param project: the Project name

            :type offset: int
            :param offset: the offset of all the matched names

            :type size: int
            :param size: the max return names count, -1 means all

            :return: ListLogStoreResponse

            :raise: LogException
            """

            # need to use extended method to get more
            if int(size) == -1 or int(size) > max_batch_size:
                return list_more(fn, int(offset), int(size), max_batch_size, self, project)

            resource_path = (root_resource and root_resource.rstrip('/')) or "/" + pluralize(entity_name)
            return do_list(self, project, resource_path, offset=offset, size=size)

    else:
        def fn(self, project, logstore, offset=0, size=100):
            """ list the {entity_title}, get first 100 items by default
            Unsuccessful opertaion will cause an LogException.

            :type project: string
            :param project: the Project name

            :type logstore: string
            :param logstore: the logstore name

            :type offset: int
            :param offset: the offset of all the matched names

            :type size: int
            :param size: the max return names count, -1 means all

            :return: ListLogStoreResponse

            :raise: LogException
            """

            # need to use extended method to get more
            if int(size) == -1 or int(size) > max_batch_size:
                return list_more(fn, int(offset), int(size), max_batch_size, self, project, logstore)

            resource_path = (root_resource and root_resource.rstrip('/')) or "/" + pluralize(entity_name)
            resource_path = "/logstores/" + logstore + resource_path
            return do_list(self, project, resource_path, offset=offset, size=size)

    fn.__name__ = 'list_' + entity_name
    fn.__doc__ = fn.__doc__.format(entity_title=entity_name.title())

    return fn


def update_entity(entity_name, logstore_level=None, name_field=None, root_resource=None):
    def do_update(self, project, detail, resource_prefix=None):
        params = {}
        headers = {}

        # parse entity value
        entity = None
        if hasattr(detail, 'to_json'):
            detail = detail.to_json()
            body_str = six.b(json.dumps(detail))
            entity = detail.get(name_field or 'name', '')
        elif isinstance(detail, six.binary_type):
            body_str = detail
        elif isinstance(detail, six.text_type):
            body_str = detail.encode('utf8')
        else:
            body_str = six.b(json.dumps(detail))
            entity = detail.get(name_field or 'name', '')

        if entity is None:
            entity = json.loads(body_str).get(name_field, '')

        assert entity, LogException('InvalidParameter', 'unknown entity name "{0}" in "{1}"'.format(name_field, detail))
        resource_path = ((root_resource and root_resource.rstrip('/')) or ('/' + pluralize(entity_name))) + '/' + entity
        if resource_prefix:
            resource_path = resource_prefix + resource_path

        headers['Content-Type'] = 'application/json'
        headers['x-log-bodyrawsize'] = str(len(body_str))
        (resp, headers) = self._send("PUT", project, body_str, resource_path, params, headers)
        return UpdateEntityResponse(headers, resp)

    if not logstore_level:
        def fn(self, project, detail):
            """ Update {entity_title}.
            Unsuccessful opertaion will cause an LogException.

            :type project: string
            :param project: project name

            :type detail: dict/string
            :param detail: json string

            :return: UpdateEntityResponse

            :raise: LogException
            """
            return do_update(self, project, detail)
    else:
        def fn(self, project, logstore, detail):
            """ Update {entity_title}.
            Unsuccessful opertaion will cause an LogException.

            :type project: string
            :param project: project name

            :type logstore: string
            :param logstore: logstore name

            :type detail: dict/string
            :param detail: json string

            :return: UpdateEntityResponse

            :raise: LogException
            """
            resource_prefix = "/logstores/" + logstore
            return do_update(self, project, detail, resource_prefix)

    fn.__name__ = 'update_' + entity_name
    fn.__doc__ = fn.__doc__.format(entity_title=entity_name.title())

    return fn


def make_lcrud_methods(obj, entity_name, logstore_level=None, name_field=None, root_resource=None, entities_key=None, raw_resource_name=None, job_type=None):
    setattr(obj, 'list_' + entity_name, list_entity(entity_name, logstore_level=logstore_level, root_resource=root_resource, entities_key=entities_key, raw_resource_name=raw_resource_name, job_type=job_type))
    setattr(obj, 'get_' + entity_name, get_entity(entity_name, logstore_level=logstore_level, root_resource=root_resource))
    setattr(obj, 'delete_' + entity_name, delete_entity(entity_name, logstore_level=logstore_level, root_resource=root_resource))
    setattr(obj, 'update_' + entity_name, update_entity(entity_name, logstore_level=logstore_level, root_resource=root_resource, name_field=name_field))
    setattr(obj, 'create_' + entity_name, create_entity(entity_name, logstore_level=logstore_level, root_resource=root_resource))
