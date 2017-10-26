#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

__all__ = ['CreateProjectResponse', 'DeleteProjectResponse', 'GetProjectResponse']


from aliyun.log.logresponse import LogResponse


class CreateProjectResponse(LogResponse):
    def __init__(self, header):
        LogResponse.__init__(self, header)

    def log_print(self):
        print('CreateProjectResponse:')
        print('headers:', self.get_all_headers())


class DeleteProjectResponse(LogResponse):
    def __init__(self, header):
        LogResponse.__init__(self, header)

    def log_print(self):
        print('DeleteProjectResponse:')
        print('headers:', self.get_all_headers())


class GetProjectResponse(LogResponse):
    def __init__(self, resp, header):
        LogResponse.__init__(self, header)
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
