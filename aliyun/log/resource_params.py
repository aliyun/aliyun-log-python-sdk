import json
from aliyun.log.util import Util


def check_type_for_init(**kwargs):
    type_dict = kwargs["instance"].type_dict
    del kwargs["instance"]
    for key, value in kwargs.items():
        if value is not None:
            need_type = type_dict.get(key)
            if not isinstance(value, need_type):
                raise TypeError("the %s type must be %s" % (key, need_type))


def check_params(name, need_type):
    def outer(func):
        def inner(*args, **kwargs):
            value = None
            if len(args) > 1:
                value = args[1]
            if kwargs:
                value = kwargs.get(name)
            if value is not None:
                if not isinstance(value, need_type):
                    raise TypeError("the %s type must be %s" % (name, need_type))
            func(*args, **kwargs)

        return inner

    return outer


class ResourceRecord:
    """
     ResourceRecord
     Create required:value
     Update required:id,value

    :type value: dict
    :param value: record value

    :type record_id: string
    :param record_id: record id

    :type tag: string
    :param tag: record tag
     """

    type_dict = {
        "record_id": str,
        "tag": str,
        "value": dict
    }

    def __init__(self, record_id=None, tag=None, value=None):
        check_type_for_init(record_id=record_id, tag=tag, value=value, instance=self)
        self.record_id = record_id
        self.tag = tag
        self.value = value
        self.create_time = None
        self.last_modify_time = None

    def get_record_id(self):
        return self.record_id

    @check_params("record_id", str)
    def set_record_id(self, record_id):
        self.record_id = record_id

    def get_tag(self):
        return self.tag

    @check_params("tag", str)
    def set_tag(self, tag):
        self.tag = tag

    def get_value(self):
        return self.value

    @check_params("tag", dict)
    def set_value(self, value):
        self.value = value

    def get_create_time(self):
        return self.create_time

    def set_create_time(self, create_time):
        self.create_time = create_time

    def get_last_modify_time(self):
        return self.last_modify_time

    def set_last_modify_time(self, last_modify_time):
        self.last_modify_time = last_modify_time

    def to_dict(self):
        result = {"value": self.value}
        if self.tag:
            result["tag"] = self.tag
        if self.record_id:
            result["id"] = self.record_id
        if self.create_time:
            result["create_time"] = self.create_time
        if self.last_modify_time:
            result["last_modify_time"] = self.last_modify_time
        return result

    def check_value(self):
        if not self.value:
            raise ValueError("ResourceRecord Value must not be None")

    def check_for_update(self):
        self.check_value()
        if not self.record_id:
            raise ValueError(" ResourceRecord id must not be None")

    @classmethod
    def from_dict(cls, dict_data):
        return ResourceRecord(record_id=dict_data.get("id"), tag=dict_data.get("tag"),
                              value=json.loads(dict_data.get("value")))


class ResourceSchemaItem:
    """
    ResourceSchemaItem
   :type column: string
   :param column: column

   :type ext_info: string
   :param ext_info: schema ext_info

   :type schema_type: string
   :param schema_type: schema_type
    """
    type_dict = {
        "column": str,
        "ext_info": str,
        "schema_type": str
    }

    def __init__(self, column=None, ext_info=None, schema_type=None):
        check_type_for_init(column=column, schema_type=schema_type, instance=self)
        self.column = column
        self.ext_info = ext_info
        self.type = schema_type

    def get_column(self):
        return self.column

    @check_params("column", str)
    def set_column(self, column):
        self.column = column

    def get_ext_info(self):
        return self.ext_info

    def set_ext_info(self, ext_info):
        self.ext_info = ext_info

    def get_schema_type(self):
        return self.type

    @check_params("schema_type", str)
    def set_schema_type(self, schema_type):
        self.type = schema_type

    def to_dict(self):
        schema_dict = {}
        if self.get_column():
            schema_dict["column"] = self.get_column()
        if self.get_ext_info():
            schema_dict["ext_info"] = self.get_ext_info()
        if self.get_schema_type():
            schema_dict["type"] = self.get_schema_type()
        return schema_dict


class Resource:
    """ Resource
    Create required: resource_name resource_type
    Update required: resource_name
    :type resource_name: string
    :param resource_name: resource name

    :type resource_type: string
    :param resource_type: resource type

    :type description: string
    :param description: the description of resource name

    :type schema_list: List[ResourceSchemaItem]
    :param schema_list: schema of resource

    :type acl: dict
    :param acl: policy  example:{"policy": {"type": "all_rw"}}

    :type ext_info: string
    :param ext_info: extra info of this resource
    """

    type_dict = {
        "resource_name": str,
        "resource_type": str,
        "schema_list": list,
        "description": str,
        "acl": dict,
        "ext_info": str
    }

    def __init__(self, resource_name=None, resource_type=None, schema_list=None,
                 description=None, acl=None, ext_info=None):
        check_type_for_init(resource_name=resource_name, resource_type=resource_type, schema_list=schema_list,
                            description=description, acl=acl, ext_info=ext_info, instance=self)
        self.resource_name = resource_name
        self.resource_type = resource_type
        self.description = description
        self.schema_list = self._check_schema(schema_list) if schema_list else schema_list
        self.acl = acl
        self.ext_info = ext_info
        self.create_time = None
        self.last_modify_time = None

    def get_resource_name(self):
        return self.resource_name

    @check_params("resource_name", str)
    def set_resource_name(self, resource_name):
        self.resource_name = resource_name

    def get_resource_type(self):
        return self.resource_type

    @check_params("resource_type", str)
    def set_resource_type(self, resource_type):
        self.resource_type = resource_type

    def get_description(self):
        return self.description

    @check_params("description", str)
    def set_description(self, description):
        self.description = description

    def get_schema(self):
        return self.schema_list

    @check_params("description", list)
    def set_schema_list(self, schema):
        self.schema_list = self._check_schema(schema) if schema else schema

    def get_acl(self):
        return self.acl

    @check_params("acl", dict)
    def set_acl(self, acl):
        self.acl = acl

    def get_ext_info(self):
        return self.ext_info

    @check_params("ext_info", str)
    def set_ext_info(self, ext_info):
        self.ext_info = ext_info

    def get_create_time(self):
        return self.create_time

    def set_create_time(self, create_time):
        self.create_time = create_time

    def get_last_modify_time(self):
        return self.last_modify_time

    def set_last_modify_time(self, last_modify_time):
        self.last_modify_time = last_modify_time

    def check_for_update(self):
        if not self.resource_name:
            raise ValueError("resource_name must not be None!")

    def check_for_create(self):
        if not (self.resource_name and self.resource_type):
            raise ValueError("resource_name and resource_type must not be None!")

    @staticmethod
    def _check_schema(schema_list):
        for schema in schema_list:
            if not isinstance(schema, ResourceSchemaItem):
                raise TypeError("schema list element must be instance of ResourceSchemaItem ")
        return schema_list

    def to_dict(self):
        schema_list = [schema.to_dict() for schema in self.schema_list]
        result = {"schema": schema_list}
        if self.create_time:
            result["create_time"] = self.create_time
        if self.last_modify_time:
            result["last_modify_time"] = self.last_modify_time
        if self.resource_name:
            result["resource_name"] = self.resource_name
        if self.resource_type:
            result["resource_type"] = self.resource_type
        if self.description:
            result["description"] = self.description
        if self.ext_info:
            result["ext_info"] = self.ext_info
        if self.acl:
            result["acl"] = self.acl
        return result

    @classmethod
    def from_dict(cls, dict_data):
        schema_list = Util.convert_unicode_to_str(json.loads(dict_data.get("schema"))).get("schema")
        schema_instance_list = []
        if schema_list:
            for schema in schema_list:
                schema_instance_list.append(ResourceSchemaItem(column=schema.get("column"), schema_type=schema.get("type"),
                                                               ext_info=schema.get("ext_info")))
        resource = Resource()
        resource.set_resource_name(dict_data.get("name"))
        resource.set_description(dict_data.get('description'))
        resource.set_resource_type(dict_data.get("type"))
        resource.set_ext_info(dict_data.get("extInfo"))
        resource.set_acl(json.loads(dict_data.get("acl")))
        resource.set_schema_list(schema_instance_list)
        resource.set_create_time(dict_data.get("create_time"))
        resource.set_last_modify_time(dict_data.get("last_modify_time"))
        return resource
