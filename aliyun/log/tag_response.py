#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

from .logresponse import LogResponse

__all__ = [
    'GetResourceTagsResponse'
]


class Tag(object):

    def __init__(self, resource_id, resource_type, tag_key, tag_value):
        self.resourc_id = resource_id
        self.resource_type = resource_type
        self.tag_key = tag_key
        self.tag_value = tag_value

    def get_resource_id(self):
        return self.resourc_id

    def get_resource_type(self):
        return self.resource_type

    def get_tag_key(self):
        return self.tag_key

    def get_tag_value(self):
        return self.tag_value


class GetResourceTagsResponse(LogResponse):
    def __init__(self, header, resp):
        LogResponse.__init__(self, header, resp)
        self._next_token = resp["nextToken"]
        self._tags = {Tag(tag["resourceId"], tag["resourceType"], tag["tagKey"], tag["tagValue"]) for tag in resp["tagResources"]}

    def get_tags(self):
        return self._tags

    @property
    def next_token(self):
        return self._next_token
