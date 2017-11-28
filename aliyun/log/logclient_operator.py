from .logexception import LogException
from functools import wraps
import six


def copy_project(from_client, to_client, from_project, to_project, copy_machine_group=False):
    """
    copy project, logstore, machine group and logtail config to target project,
    expecting the target project doens't exist
    :type from_client: LogClient
    :param from_client: logclient instance

    :type to_client: LogClient
    :param to_client: logclient instance

    :type from_project: string
    :param from_project: project name

    :type to_project: string
    :param to_project: project name

    :type copy_machine_group: bool
    :param copy_machine_group: if copy machine group resources, False by default.


    :return:
    """

    # copy project
    ret = from_client.get_project(from_project)
    ret = to_client.create_project(to_project, ret.get_description())

    default_fetch_size = 100

    # list logstore and copy them
    offset, size = 0, default_fetch_size
    while True:
        ret = from_client.list_logstore(from_project, offset=offset, size=size)
        count = ret.get_logstores_count()
        total = ret.get_logstores_total()
        for logstore_name in ret.get_logstores():
            # copy logstore
            ret = from_client.get_logstore(from_project, logstore_name)
            ret = to_client.create_logstore(to_project, logstore_name, ret.get_ttl(), ret.get_shard_count())

            # copy index
            try:
                ret = from_client.get_index_config(from_project, logstore_name)
                ret = to_client.create_index(to_project, logstore_name, ret.get_index_config())
            except LogException as ex:
                if ex.get_error_code() == 'IndexConfigNotExist':
                    pass

        offset += count
        if count < size or offset >= total:
            break

    # list logtail config and copy them
    offset, size = 0, default_fetch_size
    while True:
        ret = from_client.list_logtail_config(from_project, offset=offset, size=size)
        count = ret.get_configs_count()
        total = ret.get_configs_total()

        for config_name in ret.get_configs():
            ret = from_client.get_logtail_config(from_project, config_name)
            ret = to_client.create_logtail_config(to_project, ret.logtail_config)

        offset += count
        if count < size or offset >= total:
            break

    # list machine group and copy them
    offset, size = 0, default_fetch_size
    while copy_machine_group:
        ret = from_client.list_machine_group(from_project, offset=offset, size=size)
        count = ret.get_machine_group_count()
        total = ret.get_machine_group_total()

        for group_name in ret.get_machine_group():
            ret = from_client.get_machine_group(from_project, group_name)
            ret = to_client.create_machine_group(to_project, ret.get_machine_group())

            # list all applied config and copy the relationship
            ret = from_client.get_machine_group_applied_configs(from_project, group_name)
            for config_name in ret.get_configs():
                to_client.apply_config_to_machine_group(to_project, config_name, group_name)

        offset += count
        if count < size or offset >= total:
            break


def list_more(fn, offset, size, batch_size, *args):
    """list all data using the fn
    """
    if size < 0:
        expected_total_size = six.MAXSIZE
    else:
        expected_total_size = size
        batch_size = min(size, batch_size)

    response = None
    total_count_got = 0
    while True:
        ret = fn(*args, offset=offset, size=batch_size)
        if response is None:
            response = ret
        else:
            response.merge(ret)

        count = ret.get_count()
        total = ret.get_total()
        offset += count
        total_count_got += count
        if count == 0 or offset >= total or total_count_got >= expected_total_size:
            break

    return response


def list_logstore_all(client, project):
    """
    list all project
    :type client: LogClient
    :param client: logclient instance

    :return:
    """

    default_fetch_size = 100

    # list logstore and copy them
    offset, size = 0, default_fetch_size
    response = None
    while True:
        ret = client.list_logstores(project, offset=offset, size=size)
        if response is None:
            response = ret
        else:
            response.merge(ret)

        count = ret.get_count()
        total = ret.get_total()
        offset += count
        if count < size or offset >= total:
            break


def list_logtail_config_all(client, project):
    """
    list all project
    :type client: LogClient
    :param client: logclient instance

    :return:
    """

    default_fetch_size = 100

    # list logstore and copy them
    offset, size = 0, default_fetch_size
    response = None
    while True:
        ret = client.list_logtail_config(project, offset=offset, size=size)
        if response is None:
            response = ret
        else:
            response.merge(ret)

        count = ret.get_count()
        total = ret.get_total()
        offset += count
        if count < size or offset >= total:
            break

