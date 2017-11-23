#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.


from .logresponse import LogResponse
from .util import Util

__all__ = ['CreateProjectResponse', 'DeleteProjectResponse', 'GetProjectResponse', 'ListProjectResponse']


class CreateProjectResponse(LogResponse):
    """
    Response of create_project
    """
    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('CreateProjectResponse:')
        print('headers:', self.get_all_headers())


class DeleteProjectResponse(LogResponse):
    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('DeleteProjectResponse:')
        print('headers:', self.get_all_headers())


class GetProjectResponse(LogResponse):
    def __init__(self, resp, header):
        LogResponse.__init__(self, header, resp)
        self.status = resp['status']
        self.description = resp['description']
        self.projectName = resp['projectName']
        self.region = resp['region']
        self.owner = resp['owner']
        self.createTime = resp['createTime']
        self.lastModifyTime = resp['lastModifyTime']

    def get_owner(self):
        return self.owner

    def get_status(self):
        return self.status

    def get_description(self):
        return self.description

    def get_projectname(self):
        return self.projectName

    def get_region(self):
        return self.region

    def get_create_time(self):
        return self.createTime

    def get_last_modify_time(self):
        return self.lastModifyTime

    def log_print(self):
        print('GetProjectResponse:')
        print('headers:', self.get_all_headers())
        print('owner:' + self.get_owner())
        print('project:' + self.get_projectname())
        print('description:' + self.get_description())
        print('region:' + self.get_region())
        print('status:' + self.get_status())
        print('create_time:' + self.get_create_time())
        print('last_modify_time:' + self.get_last_modify_time())


class ListProjectResponse(LogResponse):
    def __init__(self, resp, header):
        LogResponse.__init__(self, header, resp)
        self.count = int(resp['count'])
        self.total = int(resp['total'])
        self.projects = Util.convert_unicode_to_str(resp.get("projects", []))

    def get_count(self):
        return self.count

    def get_total(self):
        return self.total

    def get_projects(self):
        return self.projects

    def log_print(self):
        print('ListProjectResponse:')
        print('headers:', self.get_all_headers())
        print('count:', self.count)
        print('total:', self.total)
        print('projects:', self.get_projects())

    def merge(self, response):
        if not isinstance(response, ListProjectResponse):
            raise ValueError("passed response is not a ListProjectResponse: " + str(type(response)))

        self.count += response.get_count()
        self.total = response.get_total() # use the latest total count
        self.projects.extend(response.get_projects())

        # update body
        self.body = {
            'count': self.count,
            'total': self.total,
            'projects': self.projects
        }

        return self
