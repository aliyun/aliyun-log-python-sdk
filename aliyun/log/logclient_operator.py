from .logexception import LogException
import six
import json
from aliyun.log import *
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing
from .logresponse import LogResponse
from json import JSONEncoder

MAX_INIT_SHARD_COUNT = 10


def copy_project(from_client, to_client, from_project, to_project, copy_machine_group=False):
    """
    copy project, logstore, machine group and logtail config to target project,
    will create the target project if it doesn't exist

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
    try:
        ret = to_client.create_project(to_project, ret.get_description())
    except LogException as ex:
        if ex.get_error_code() == 'ProjectAlreadyExist':
            # don't create the project as it already exists
            pass
        else:
            raise

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
            ret = to_client.create_logstore(to_project, logstore_name, ret.get_ttl(),
                                            min(ret.get_shard_count(), MAX_INIT_SHARD_COUNT))

            # copy index
            try:
                ret = from_client.get_index_config(from_project, logstore_name)
                ret = to_client.create_index(to_project, logstore_name, ret.get_index_config())
            except LogException as ex:
                if ex.get_error_code() == 'IndexConfigNotExist':
                    pass
                else:
                    raise

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


def copy_logstore(from_client, from_project, from_logstore, to_logstore, to_project=None, to_client=None):
    """
    copy logstore, index, logtail config to target logstore, machine group are not included yet.
    the target logstore will be crated if not existing

    :type from_client: LogClient
    :param from_client: logclient instance

    :type from_project: string
    :param from_project: project name

    :type from_logstore: string
    :param from_logstore: logstore name

    :type to_logstore: string
    :param to_logstore: target logstore name

    :type to_project: string
    :param to_project: project name, copy to same project if not being specified, will try to create it if not being specified

    :type to_client: LogClient
    :param to_client: logclient instance, use it to operate on the "to_project" if being specified

    :return:
    """

    # check client
    if to_project is not None:
        # copy to a different project in different client
        to_client = to_client or from_client

        # check if target project exists or not
        ret = from_client.get_project(from_project)
        try:
            ret = to_client.create_project(to_project, ret.get_description())
        except LogException as ex:
            if ex.get_error_code() == 'ProjectAlreadyExist':
                # don't create the project as it already exists
                pass
            else:
                raise

    to_project = to_project or from_project
    to_client = to_client or from_client

    # return if logstore are the same one
    if from_client is to_client and from_project == to_project and from_logstore == to_logstore:
        return

    # copy logstore
    ret = from_client.get_logstore(from_project, from_logstore)
    try:
        ret = to_client.create_logstore(to_project, to_logstore,
                                     ttl=ret.get_ttl(),
                                     shard_count=min(ret.get_shard_count(), MAX_INIT_SHARD_COUNT),
                                     enable_tracking=ret.get_enable_tracking())
    except LogException as ex:
        if ex.get_error_code() == 'LogStoreAlreadyExist':
            # update logstore's settings
            ret = to_client.update_logstore(to_project, to_logstore,
                                            ttl=ret.get_ttl(),
                                            shard_count=min(ret.get_shard_count(), MAX_INIT_SHARD_COUNT),
                                            enable_tracking=ret.get_enable_tracking())
        else:
            raise

    # copy index
    try:
        ret = from_client.get_index_config(from_project, from_logstore)
        ret = to_client.create_index(to_project, to_logstore, ret.get_index_config())
    except LogException as ex:
        if ex.get_error_code() == 'IndexConfigNotExist':
            # source has no index
            pass
        elif ex.get_error_code() == 'IndexAlreadyExist':
            # target already has index, overwrite it
            ret = to_client.update_index(to_project, to_logstore, ret.get_index_config())
            pass
        else:
            raise

    # list logtail config linked to the logstore and copy them
    default_fetch_size = 100
    offset, size = 0, default_fetch_size
    while True:
        ret = from_client.list_logtail_config(from_project, offset=offset, size=size)
        count = ret.get_configs_count()
        total = ret.get_configs_total()

        for config_name in ret.get_configs():
            ret = from_client.get_logtail_config(from_project, config_name)
            config = ret.logtail_config
            if config.logstore_name != from_logstore:
                continue

            config.config_name = to_logstore + '_' + config_name
            config.logstore_name = to_logstore
            ret = to_client.create_logtail_config(to_project, config)

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
        batch_size = min(batch_size, expected_total_size - total_count_got)

        if count == 0 or offset >= total or total_count_got >= expected_total_size:
            break

    return response


def query_more(fn, offset, size, batch_size, *args):
    """list all data using the fn
    """
    if size < 0:
        expected_total_size = six.MAXSIZE
    else:
        expected_total_size = size
        batch_size = min(size, batch_size)

    response = None
    total_count_got = 0
    complete = False
    while True:
        ret = fn(*args, offset=offset, size=batch_size)

        if response is None:
            response = ret
        else:
            response.merge(ret)

        # if incompete, exit
        if not ret.is_completed():
            break

        count = ret.get_count()
        offset += count
        total_count_got += count
        batch_size = min(batch_size, expected_total_size - total_count_got)
        if count == 0 or total_count_got >= expected_total_size:
            break

    return response


def get_encoder_cls(encodings):
    class NonUtf8Encoder(JSONEncoder):
        def default(self, obj):
            if isinstance(obj, six.binary_type):
                for encoding in encodings:
                    try:
                        return obj.decode(encoding)
                    except UnicodeDecodeError as ex:
                        pass
                return obj.decode('utf8', "ignore")

            return JSONEncoder.default(self, obj)

    return NonUtf8Encoder


def worker(client, project_name, logstore_name, from_time, to_time,
           shard_id, file_path,
           batch_size=1000, compress=True, encodings=None):
    res = client.pull_log(project_name, logstore_name, shard_id, from_time, to_time, batch_size=batch_size,
                          compress=compress)
    encodings = encodings or ('utf8', 'latin1', 'gbk')

    count = 0
    for data in res:
        for log in data.get_flatten_logs_json():
            with open(file_path, "a+") as f:
                count += 1

                if six.PY2:
                    last_ex = None
                    for encoding in encodings:
                        try:
                            f.write(json.dumps(log, encoding=encoding))
                            f.write("\n")
                            break
                        except UnicodeDecodeError as ex:
                            last_ex = ex
                    else:
                        raise last_ex
                else:
                    f.write(json.dumps(log, cls=get_encoder_cls(encodings)))
                    f.write("\n")


    return file_path, count


def pull_log_dump(client, project_name, logstore_name, from_time, to_time, file_path, batch_size=500, compress=True, encodings=None):
    cpu_count = multiprocessing.cpu_count() * 2
    shards = client.list_shards(project_name, logstore_name).get_shards_info()
    worker_size = min(cpu_count, len(shards))

    result = dict()
    total_count = 0
    with ProcessPoolExecutor(max_workers=worker_size) as pool:
        futures = [pool.submit(worker, client, project_name, logstore_name, from_time, to_time,
                               shard_id=shard['shardID'], file_path=file_path.format(shard['shardID']),
                               batch_size=batch_size, compress=compress, encodings=encodings)
                   for shard in shards]

        for future in as_completed(futures):
            file_path, count = future.result()
            total_count += count
            if count:
                result[file_path] = count

    return LogResponse({}, {"total_count": total_count, "files": result})
