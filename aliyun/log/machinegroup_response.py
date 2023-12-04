#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

__all__ = ['CreateMachineGroupResponse', 'DeleteMachineGroupResponse',
           'GetMachineGroupResponse', 'UpdateMachineGroupResponse',
           'ListMachineGroupResponse', 'ListMachinesResponse',
           'ApplyConfigToMachineGroupResponse', 'RemoveConfigToMachineGroupResponse',
           'GetMachineGroupAppliedConfigResponse', 'GetConfigAppliedMachineGroupsResponse']

from .util import Util
from .logresponse import LogResponse
from .machine_group_detail import MachineGroupDetail
from .machine_group_detail import MachineStatus


class CreateMachineGroupResponse(LogResponse):
    """ The response of the create_machine_group API from log.

    :type header: dict
    :param header: CreateMachineGroupResponse HTTP response header
    """

    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('CreateMachineGroupResponse:')
        print('headers:', self.get_all_headers())


class DeleteMachineGroupResponse(LogResponse):
    """ The response of the delete_machine_group API from log.

    :type header: dict
    :param header: DeleteMachineGroupResponse HTTP response header
    """

    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('DeleteMachineGroupResponse:')
        print('headers:', self.get_all_headers())


class GetMachineGroupResponse(LogResponse):
    """ The response of the get_machine_group API from log.

    :type header: dict
    :param header: GetMachineGroupResponse HTTP response header

    :type resp: dict
    :param resp: the HTTP response body
    """

    def __init__(self, resp, header):
        LogResponse.__init__(self, header, resp)
        self.machine_group = MachineGroupDetail(None, None, None)
        self.machine_group.from_json(resp)

    def get_machine_group(self):
        """

        :return:
        """
        return self.machine_group

    def log_print(self):
        """

        :return:
        """
        print('GetMachineGroupResponse:')
        print('headers:', self.get_all_headers())
        print('machine_group', self.machine_group.to_json())


class UpdateMachineGroupResponse(LogResponse):
    """ The response of the update_machine_group API from log.

    :type header: dict
    :param header: UpdateMachineGroupResponse HTTP response header
    """

    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('UpdateMachineGroupResponse:')
        print('headers:', self.get_all_headers())


class ListMachineGroupResponse(LogResponse):
    """ The response of the list_machine_group API from log.

    :type header: dict
    :param header: ListMachineGroupResponse HTTP response header

    :type resp: dict
    :param resp: the HTTP response body
    """

    def __init__(self, resp, header):
        LogResponse.__init__(self, header, resp)
        self.count = int(resp["count"])
        self._total = int(resp["total"])
        self.machine_groups = Util.convert_unicode_to_str(resp.get("machinegroups", []))

    def get_machine_group(self):
        """

        :return:
        """
        return self.machine_groups

    def get_machine_group_count(self):
        """

        :return:
        """
        return self.count

    def get_machine_group_total(self):
        """

        :return:
        """
        return self._total

    def get_count(self):
        return self.count

    def get_total(self):
        return self._total

    @property
    def total(self):
        return self._total

    def log_print(self):
        """

        :return:
        """
        print('ListMachineGroupResponse:')
        print('headers:', self.get_all_headers())
        print('count:', str(self.count))
        print('total:', str(self._total))
        print('machine_groups:', str(self.machine_groups))

    def merge(self, response):
        if not isinstance(response, ListMachineGroupResponse):
            raise ValueError("passed response is not a ListMachineGroupResponse: " + str(type(response)))

        self.count += response.get_count()
        self._total = response.get_total()  # use the latest total count
        self.machine_groups.extend(response.get_machine_group())

        # update body
        self.body = {
            'count': self.count,
            'total': self._total,
            'machinegroups': self.machine_groups
        }

        return self


class ListMachinesResponse(LogResponse):
    """ The response of the list_machines API from log.

    :type header: dict
    :param header: ListMachinesResponse HTTP response header

    :type resp: dict
    :param resp: the HTTP response body
    """

    def __init__(self, resp, header):
        LogResponse.__init__(self, header, resp)
        self.count = resp['count']
        self.total = resp['total']
        self.machines = []
        for machine_status in resp['machines']:
            machine = MachineStatus(machine_status['ip'], machine_status['machine-uniqueid'],
                                    machine_status['userdefined-id'], machine_status['lastHeartbeatTime'])
            self.machines.append(machine)

    def get_machine_count(self):
        """

        :return:
        """
        return self.count

    def get_machine_total(self):
        """

        :return:
        """
        return self.total

    def get_machines(self):
        """

        :return:
        """
        return self.machines

    def get_count(self):
        return self.count

    def get_total(self):
        return self.total

    def log_print(self):
        print('GetMachineGroupResponse:')
        print('headers:', self.get_all_headers())
        print('machine_count', self.count)
        print('machine_total', self.total)
        print('machines:', self.machines)

    def merge(self, response):
        if not isinstance(response, ListMachinesResponse):
            raise ValueError("passed response is not a ListMachineGroupResponse: " + str(type(response)))

        self.count += response.get_count()
        self.total = response.get_total()  # use the latest total count
        self.machines.extend(response.get_machines())

        # update body
        self.body['count'] = self.count
        self.body['total'] = self.total
        self.body['machines'].extend(response.get_body()['machines'])

        return self


class ApplyConfigToMachineGroupResponse(LogResponse):
    """ The response of the apply_config_to_machine_group API from log.

    :type header: dict
    :param header: ApplyConfigToMachineGroupResponse HTTP response header
    """

    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('ApplyConfigToMachineGroupResponse:')
        print('headers:', self.get_all_headers())


class RemoveConfigToMachineGroupResponse(LogResponse):
    """ The response of the remove_config_to_machine_group API from log.

    :type header: dict
    :param header: RemoveConfigToMachineGroupResponse HTTP response header
    """

    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('RemoveConfigToMachineGroupResponse:')
        print('headers:', self.get_all_headers())


class GetMachineGroupAppliedConfigResponse(LogResponse):
    """ The response of the get_machine_group_applied_config API from log.

    :type header: dict
    :param header: GetMachineGroupAppliedConfigResponse HTTP response header

    :type resp: dict
    :param resp: the HTTP response body
    """

    def __init__(self, resp, header):
        LogResponse.__init__(self, header, resp)
        self.count = resp['count']
        self.configs = resp['configs']

    def get_config_count(self):
        """

        :return:
        """
        return self.count

    def get_configs(self):
        """

        :return:
        """
        return self.configs

    def log_print(self):
        print('GetMachineGroupAppliedConfigResponse:')
        print('headers:', self.get_all_headers())
        print('count:', self.count)
        print('configs:', self.configs)


class GetConfigAppliedMachineGroupsResponse(LogResponse):
    """ The response of the get_config_applied_machine_group API from log.

    :type header: dict
    :param header: GetConfigAppliedMachineGroupsResponse HTTP response header

    :type resp: dict
    :param resp: the HTTP response body
    """

    def __init__(self, resp, header):
        LogResponse.__init__(self, header, resp)
        self.count = resp['count']
        self.machine_groups = resp['machinegroups']

    def get_machine_group_count(self):
        """

        :return:
        """
        return self.count

    def get_machine_groups(self):
        """

        :return:
        """
        return self.machine_groups

    def log_print(self):
        """

        :return:
        """
        print('GetConfigAppliedMachineGroupsResponse:')
        print('headers:', self.get_all_headers())
        print('count:', self.count)
        print('machine_groups:', self.machine_groups)
