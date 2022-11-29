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


class Topostore:
    """ Topostore
    Create required: name
    Update required: name
    :type name: string
    :param name: topostore name

    :type tag: dict
    :param tag: topostore tag

    :type description: string
    :param description: the description of topostore

    :type schema: TopostoreSchema
    :param schema: schema of topostore node and relations

    :type acl: dict
    :param acl: policy  example:{"policy": {"type": "all_rw"}}

    :type ext_info: dict
    :param ext_info: extra info of this topostore
    """

    type_dict = {
        "name": str,
        "tag": dict,
        "schema": dict,
        "description": str,
        "acl": dict,
        "ext_info": str
    }

    def __init__(self, name=None, tag=None, schema=None,
                 description=None, acl=None, ext_info=None):
        check_type_for_init(name=name, tag=tag, schema=schema,
                            description=description, acl=acl, ext_info=ext_info, instance=self)
        self.name = name
        self.tag = tag
        self.description = description
        self.schema = schema
        self.acl = acl
        self.ext_info = ext_info
        self.create_time = None
        self.last_modify_time = None

    def get_name(self):
        return self.name

    @check_params("name", str)
    def set_name(self, name):
        self.name = name

    def get_tag(self):
        return self.tag

    @check_params("tag", dict)
    def set_tag(self, tag):
        self.tag = tag

    def get_description(self):
        return self.description

    @check_params("description", str)
    def set_description(self, description):
        self.description = description

    def get_schema(self):
        return self.schema

    @check_params("schema", dict)
    def set_schema(self, schema):
        self.schema = schema

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
        if not self.name:
            raise ValueError("topostore name must not be None!")

    def check_for_create(self):
        if not (self.name):
            raise ValueError("topostore name must not be None!")


    def to_dict(self):
        result = {"schema": self.schema}
        if self.create_time:
            result["create_time"] = self.create_time
        if self.last_modify_time:
            result["last_modify_time"] = self.last_modify_time
        if self.name:
            result["name"] = self.name
        if self.tag:
            result["tag"] = self.tag
        if self.description:
            result["description"] = self.description
        if self.ext_info:
            result["ext_info"] = self.ext_info
        if self.acl:
            result["acl"] = self.acl
        return result

    @classmethod
    def from_dict(cls, dict_data):
        topostore = Topostore()
        topostore.set_name(dict_data.get("name"))
        topostore.set_description(dict_data.get('description'))
        topostore.set_tag(json.loads(dict_data.get("tag")))
        topostore.set_ext_info(dict_data.get("extInfo"))
        topostore.set_acl(json.loads(dict_data.get("acl")))
        topostore.set_schema(json.loads(dict_data.get("schema")))
        topostore.set_create_time(dict_data.get("create_time"))
        topostore.set_last_modify_time(dict_data.get("last_modify_time"))
        return topostore

class TopostoreNode:
    """ Node of topostore
    Create required: node_id
    Update required: node_id

    :type node_id: string
    :param node_id: node_id

    :type node_type: string
    :param node_type: the type of node

    :type property: dict
    :param property: node property

    :type description: string
    :param description: the description of node

    :type display_name: string
    :param display_name: the display name of node
    """

    type_dict = {
        "node_id": str,
        "node_type": str,
        "property": dict,
        "description": str,
        "display_name": str,
    }

    def __init__(self, node_id=None, node_type=None, property=None,
                 description=None, display_name=None):
        check_type_for_init(node_id=node_id, node_type=node_type, property=property,
                            description=description, display_name=display_name, instance=self)
        self.node_id = node_id
        self.node_type = node_type
        self.description = description
        self.property = property
        self.display_name = display_name
        self.create_time = None
        self.last_modify_time = None

    def get_node_id(self):
        return self.node_id

    @check_params("node_id", str)
    def set_node_id(self, node_id):
        self.node_id = node_id

    def get_node_type(self):
        return self.node_type

    @check_params("node_type", str)
    def set_node_type(self, node_type):
        self.node_type = node_type

    def get_description(self):
        return self.description

    @check_params("description", str)
    def set_description(self, description):
        self.description = description

    def get_property(self):
        return self.property

    @check_params("property", dict)
    def set_property(self, property):
        self.property = property

    def get_display_name(self):
        return self.display_name

    @check_params("display_name", str)
    def set_display_name(self, display_name):
        self.display_name = display_name

    def get_create_time(self):
        return self.create_time

    def set_create_time(self, create_time):
        self.create_time = create_time

    def get_last_modify_time(self):
        return self.last_modify_time

    def set_last_modify_time(self, last_modify_time):
        self.last_modify_time = last_modify_time

    def check_for_update(self):
        if not self.node_id:
            raise ValueError("node id must not be None!")

    def check_for_create(self):
        if not (self.node_id):
            raise ValueError("node id must not be None!")

    def to_dict(self):
        result = {"nodeId": self.node_id}
        if self.node_type:
            result["nodeType"] = self.node_type
        if self.create_time:
            result["createTime"] = self.create_time
        if self.last_modify_time:
            result["lastModifyTime"] = self.last_modify_time
        if self.display_name:
            result["displayName"] = self.display_name
        if self.property:
            result["property"] = self.property
        if self.description:
            result["description"] = self.description
        return result

    @classmethod
    def from_dict(cls, dict_data):
        node = TopostoreNode()
        node.set_node_id(dict_data.get("nodeId"))
        node.set_node_type(dict_data.get("nodeType"))
        node.set_property(json.loads(dict_data.get("property")))
        node.set_description(dict_data.get('description'))
        node.set_display_name(dict_data.get("displayName"))
        node.set_create_time(dict_data.get("createTime"))
        node.set_last_modify_time(dict_data.get("last_modifyTime"))
        return node


class TopostoreRelation:
    """ Relation of topostore
    Create required: relation_id
    Update required: relation_id

    :type relation_id: string
    :param relation_id: relation_id

    :type relation_type: string
    :param relation_type: the type of relation

    :type src_node_id: string
    :param src_node_id: the src_node_id of relation

    :type dst_node_id: string
    :param dst_node_id: the dst_node_id of relation

    :type property: dict
    :param property: relation property

    :type description: string
    :param description: the description of relation

    :type display_name: string
    :param display_name: the display name of relation
    """

    type_dict = {
        "relation_id": str,
        "relation_type": str,
        "src_node_id": str,
        "dst_node_id": str,
        "property": dict,
        "description": str,
        "display_name": str,
    }

    def __init__(self, relation_id=None, relation_type=None,src_node_id=None, dst_node_id=None, property=None,
                 description=None, display_name=None):
        check_type_for_init(relation_id=relation_id, relation_type=relation_type, property=property,
                            description=description, display_name=display_name, instance=self)
        self.relation_id = relation_id
        self.relation_type = relation_type
        self.src_node_id = src_node_id
        self.dst_node_id = dst_node_id
        self.description = description
        self.property = property
        self.display_name = display_name
        self.create_time = None
        self.last_modify_time = None

    def get_relation_id(self):
        return self.relation_id

    @check_params("relation_id", str)
    def set_relation_id(self, relation_id):
        self.relation_id = relation_id

    def get_relation_type(self):
        return self.relation_type

    @check_params("relation_type", str)
    def set_relation_type(self, relation_type):
        self.relation_type = relation_type

    def get_src_node_id(self):
        return self.src_node_id

    @check_params("src_node_id", str)
    def set_src_node_id(self, src_node_id):
        self.src_node_id = src_node_id

    def get_dst_node_id(self):
        return self.dst_node_id

    @check_params("dst_node_id", str)
    def set_dst_node_id(self, dst_node_id):
        self.dst_node_id = dst_node_id

    def get_description(self):
        return self.description

    @check_params("description", str)
    def set_description(self, description):
        self.description = description

    def get_property(self):
        return self.property

    @check_params("property", dict)
    def set_property(self, property):
        self.property = property

    def get_display_name(self):
        return self.display_name

    @check_params("display_name", str)
    def set_display_name(self, display_name):
        self.display_name = display_name

    def get_create_time(self):
        return self.create_time

    def set_create_time(self, create_time):
        self.create_time = create_time

    def get_last_modify_time(self):
        return self.last_modify_time

    def set_last_modify_time(self, last_modify_time):
        self.last_modify_time = last_modify_time

    def check_for_update(self):
        if not self.relation_id:
            raise ValueError("relation id must not be None!")

    def check_for_create(self):
        if not (self.relation_id):
            raise ValueError("relation id must not be None!")

    def to_dict(self):
        result = {"relationId": self.relation_id}
        if self.relation_type:
            result["relationType"] = self.relation_type
        if self.src_node_id:
            result["srcNodeId"] = self.src_node_id
        if self.dst_node_id:
            result["dstNodeId"] = self.dst_node_id
        if self.create_time:
            result["createTime"] = self.create_time
        if self.last_modify_time:
            result["lastModifyTime"] = self.last_modify_time
        if self.display_name:
            result["displayName"] = self.display_name
        if self.property:
            result["property"] = self.property
        if self.description:
            result["description"] = self.description
        return result

    @classmethod
    def from_dict(cls, dict_data):
        relation = TopostoreRelation()
        relation.set_relation_id(dict_data.get("relationId"))
        relation.set_relation_type(dict_data.get("relationType"))
        relation.set_src_node_id(dict_data.get("srcNodeId"))
        relation.set_dst_node_id(dict_data.get("dstNodeId"))
        relation.set_property(json.loads(dict_data.get("property")))
        relation.set_description(dict_data.get('description'))
        relation.set_display_name(dict_data.get("displayName"))
        relation.set_create_time(dict_data.get("createTime"))
        relation.set_last_modify_time(dict_data.get("lastModifyTime"))
        return relation
