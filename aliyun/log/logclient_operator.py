from __future__ import print_function
from .logexception import LogException
import six
import json
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing
from .logresponse import LogResponse
from json import JSONEncoder
import logging
from collections import defaultdict
import time
from .etl_core import Runner
from .putlogsrequest import PutLogsRequest
from .logitem import LogItem
from aliyun.log.pulllog_response import PullLogResponse
from .consumer import *
from multiprocessing import RLock
from .util import base64_encodestring as b64e
import copy


MAX_INIT_SHARD_COUNT = 100

logger = logging.getLogger(__name__)


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
    source_logstores, all_source_logstores = set(), set()
    while True:
        ret = from_client.list_logstore(from_project, offset=offset, size=size)
        count = ret.get_logstores_count()
        total = ret.get_logstores_total()
        source_logstores = set(ret.get_logstores())
        all_source_logstores = all_source_logstores.union(source_logstores)
        for logstore_name in source_logstores:
            # copy logstore
            ret = from_client.get_logstore(from_project, logstore_name)
            res_shard = from_client.list_shards(from_project, logstore_name)
            expected_rwshard_count = len([shard for shard in res_shard.shards if shard['status'].lower() == 'readwrite'])
            try:
                ret2 = to_client.create_logstore(to_project, logstore_name, ret.get_ttl(),
                                                min(expected_rwshard_count, MAX_INIT_SHARD_COUNT),
                                                enable_tracking=ret.get_enable_tracking(),
                                                append_meta=ret.append_meta,
                                                auto_split=ret.auto_split,
                                                max_split_shard=ret.max_split_shard,
                                                preserve_storage=ret.preserve_storage
                                                )
            except LogException as ex:
                if ex.get_error_code().lower() == "logstorealreadyexist":
                    pass
                else:
                    raise

            # copy index
            index_config = None
            try:
                ret = from_client.get_index_config(from_project, logstore_name)
                index_config = ret.get_index_config()
            except LogException as ex:
                if ex.get_error_code() == 'IndexConfigNotExist':
                    pass
                else:
                    raise

            if index_config is not None:
                for x in range(60):
                    try:
                        ret2 = to_client.create_index(to_project, logstore_name, ret.get_index_config())
                        break
                    except LogException as ex:
                        if ex.get_error_code().lower() == "logstorenotexist" and x < 59:
                            time.sleep(1)
                            continue
                        if ex.get_error_code().lower() == "indexalreadyexist":
                            # target already has index, overwrite it
                            ret2 = to_client.update_index(to_project, logstore_name, index_config)
                            break
                        raise ex

        offset += count
        if count < size or offset >= total:
            break

    # list logtail config and copy them
    offset, size = 0, default_fetch_size
    source_configs = set()
    while True:
        ret = from_client.list_logtail_config(from_project, offset=offset, size=size)
        count = ret.get_configs_count()
        total = ret.get_configs_total()

        for config_name in ret.get_configs():
            ret = from_client.get_logtail_config(from_project, config_name)
            if ret.logtail_config.logstore_name not in all_source_logstores:
                continue

            source_configs.add(config_name)
            for x in range(60):
                try:
                    ret2 = to_client.create_logtail_config(to_project, ret.logtail_config)
                except LogException as ex:
                    if ex.get_error_code().lower() == "logstorenotexist" and x < 59:
                        time.sleep(1)
                        continue
                    if ex.get_error_code().lower() == "configalreadyexist":
                        break
                    raise ex

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
            try:
                ret = to_client.create_machine_group(to_project, ret.get_machine_group())
            except LogException as ex:
                if ex.get_error_code() == 'MachineGroupAlreadyExist':
                    pass
                else:
                    raise ex

            # list all applied config and copy the relationship
            ret = from_client.get_machine_group_applied_configs(from_project, group_name)
            for config_name in ret.get_configs():
                if config_name not in source_configs:
                    continue

                for x in range(60):
                    try:
                       to_client.apply_config_to_machine_group(to_project, config_name, group_name)
                    except LogException as ex:
                        if ex.get_error_code().lower() in ("confignotexist", "groupnotexist") and x < 59:
                            time.sleep(1)
                            continue
                        raise ex

        offset += count
        if count < size or offset >= total:
            break


def copy_logstore(from_client, from_project, from_logstore, to_logstore, to_project=None, to_client=None, to_region_endpoint=None):
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

    :type to_region_endpoint: string
    :param to_region_endpoint: target region, use it to operate on the "to_project" while "to_client" not be specified

    :return:
    """

    if to_region_endpoint is not None and to_client is None:
        to_client = copy.deepcopy(from_client)
        to_client.set_endpoint(to_region_endpoint)
    else:
        to_client = to_client or from_client

    # check client
    if to_project is not None:
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

    # return if logstore are the same one
    if from_client is to_client and from_project == to_project and from_logstore == to_logstore:
        return

    # copy logstore
    ret = from_client.get_logstore(from_project, from_logstore)
    res_shard = from_client.list_shards(from_project, from_logstore)
    expected_rwshard_count = len([shard for shard in res_shard.shards if shard['status'].lower() == 'readwrite'])
    try:
        ret = to_client.create_logstore(to_project, to_logstore,
                                        ttl=ret.get_ttl(),
                                        shard_count=min(expected_rwshard_count, MAX_INIT_SHARD_COUNT),
                                        enable_tracking=ret.get_enable_tracking(),
                                        append_meta=ret.append_meta,
                                        auto_split=ret.auto_split,
                                        max_split_shard=ret.max_split_shard,
                                        preserve_storage=ret.preserve_storage)
    except LogException as ex:
        if ex.get_error_code().lower() == "logstorealreadyexist":
            # update logstore's settings
            ret = to_client.update_logstore(to_project, to_logstore,
                                            ttl=ret.get_ttl(),
                                            enable_tracking=ret.get_enable_tracking(),
                                            append_meta=ret.append_meta,
                                            auto_split=ret.auto_split,
                                            max_split_shard=ret.max_split_shard,
                                            preserve_storage=ret.preserve_storage
                                            )

            # arrange shard to expected count
            res = arrange_shard(to_client, to_project, to_logstore, min(expected_rwshard_count, MAX_INIT_SHARD_COUNT))
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


def query_more(fn, offset=0, size=100, batch_size=100, **kwargs):
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
        ret = fn(offset=offset, size=batch_size, **kwargs)

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


def dump_worker(client, project_name, logstore_name, from_time, to_time,
                shard_id, file_path,
                batch_size=None, compress=None, encodings=None, no_escape=None, query=None):
    res = client.pull_log(project_name, logstore_name, shard_id, from_time, to_time, batch_size=batch_size,
                          compress=compress, query=query)
    encodings = encodings or ('utf8', 'latin1', 'gbk')
    ensure_ansi = not no_escape

    count = 0
    next_cursor = 'as from_time configured'
    try:
        for data in res:
            for log in data.get_flatten_logs_json(decode_bytes=True):
                with open(os.path.expanduser(file_path), "a+") as f:
                    count += 1
                    try:
                        if six.PY2:
                            last_ex = None
                            for encoding in encodings:
                                try:
                                    ret = json.dumps(log, encoding=encoding, ensure_ascii=ensure_ansi)
                                    if isinstance(ret, unicode):
                                        ret = ret.encode(encoding, errors="ignore")
                                    f.write(ret)
                                    f.write("\n")
                                    break
                                except UnicodeDecodeError as ex:
                                    last_ex = ex
                            else:
                                raise last_ex
                        else:
                            f.write(json.dumps(log, cls=get_encoder_cls(encodings), ensure_ascii=ensure_ansi))
                            f.write("\n")
                    except Exception as ex:
                        logger.error("shard: {0} Fail to dump log: {1}".format(shard_id, b64e(repr(log))), exc_info=True)
                        raise ex
            next_cursor = data.next_cursor
    except Exception as ex:
        logger.error("dump log failed: task info {0} failed to copy data to target, next cursor: {1} detail: {2}".
                     format(
            (project_name, logstore_name, shard_id, from_time, to_time),
            next_cursor, ex), exc_info=True)
        raise

    return file_path, count


def pull_log_dump(client, project_name, logstore_name, from_time, to_time, file_path, batch_size=None, compress=None,
                  encodings=None, shard_list=None, no_escape=None, query=None):
    cpu_count = multiprocessing.cpu_count() * 2

    shards = client.list_shards(project_name, logstore_name).get_shards_info()
    current_shards = [str(shard['shardID']) for shard in shards]
    target_shards = _parse_shard_list(shard_list, current_shards)
    worker_size = min(cpu_count, len(target_shards))

    result = dict()
    total_count = 0
    with ProcessPoolExecutor(max_workers=worker_size) as pool:
        futures = [pool.submit(dump_worker, client, project_name, logstore_name, from_time, to_time,
                               shard_id=shard, file_path=file_path.format(shard),
                               batch_size=batch_size, compress=compress, encodings=encodings, no_escape=no_escape, query=query)
                   for shard in target_shards]

        for future in as_completed(futures):
            file_path, count = future.result()
            total_count += count
            if count:
                result[file_path] = count

    return LogResponse({}, {"total_count": total_count, "files": result})


def copy_worker(from_client, from_project, from_logstore, shard_id, from_time, to_time,
                to_client, to_project, to_logstore, batch_size=None, compress=None,
                new_topic=None, new_source=None):
    next_cursor = "As from_time configured"

    try:
        iter_data = from_client.pull_log(from_project, from_logstore, shard_id, from_time, to_time, batch_size=batch_size,
                                         compress=compress)

        count = 0
        for res in iter_data:
            for loggroup in res.get_loggroup_list().LogGroups:
                if new_topic is not None:
                    loggroup.Topic = new_topic
                if new_source is not None:
                    loggroup.Source = new_source
                rtn = to_client.put_log_raw(to_project, to_logstore, loggroup, compress=compress)
                count += len(loggroup.Logs)

            next_cursor = res.next_cursor
        return shard_id, count
    except Exception as ex:
        logger.error("copy data failed: task info {0} failed to copy data to target, next cursor: {1} detail: {2}".
                     format( (from_project, from_logstore, shard_id, from_time, to_time, to_client, to_project, to_logstore), next_cursor, ex), exc_info=True)
        raise


def _parse_shard_list(shard_list, current_shard_list):
    """
    parse shard list
    :param shard_list: format like: 1,5-10,20
    :param current_shard_list: current shard list
    :return:
    """
    if not shard_list:
        return current_shard_list
    target_shards = []
    for n in shard_list.split(","):
        n = n.strip()
        if n.isdigit() and n in current_shard_list:
            target_shards.append(n)
        elif n:
            rng = n.split("-")
            if len(rng) == 2:
                s = rng[0].strip()
                e = rng[1].strip()
                if s.isdigit() and e.isdigit():
                    for x in range(int(s), int(e)+1):
                        if str(x) in current_shard_list:
                            target_shards.append(str(x))

    logger.info("parse_shard, shard_list: '{0}' current shard '{1}' result: '{2}'".format(shard_list,
                                                                                          current_shard_list, target_shards))
    if not target_shards:
        raise LogException("InvalidParameter", "There's no available shard with settings {0}".format(shard_list))

    return target_shards


def copy_data(from_client, from_project, from_logstore, from_time, to_time=None,
              to_client=None, to_project=None, to_logstore=None,
              shard_list=None,
              batch_size=None, compress=None, new_topic=None, new_source=None):
    """
    copy data from one logstore to another one (could be the same or in different region), the time is log received time on server side.

    """
    to_client = to_client or from_client
    # increase the timeout to 2 min at least
    from_client.timeout = max(from_client.timeout, 120)
    to_client.timeout = max(to_client.timeout, 120)

    to_project = to_project or from_project
    to_logstore = to_logstore or from_logstore
    to_time = to_time or "end"

    cpu_count = multiprocessing.cpu_count() * 2
    shards = from_client.list_shards(from_project, from_logstore).get_shards_info()
    current_shards = [str(shard['shardID']) for shard in shards]
    target_shards = _parse_shard_list(shard_list, current_shards)
    worker_size = min(cpu_count, len(target_shards))

    result = dict()
    total_count = 0
    with ProcessPoolExecutor(max_workers=worker_size) as pool:
        futures = [pool.submit(copy_worker, from_client, from_project, from_logstore, shard,
                               from_time, to_time,
                               to_client, to_project, to_logstore,
                               batch_size=batch_size, compress=compress,
                               new_topic=new_topic, new_source=new_source)
                   for shard in target_shards]

        for future in as_completed(futures):
            partition, count = future.result()
            total_count += count
            if count:
                result[partition] = count

    return LogResponse({}, {"total_count": total_count, "shards": result})


TOTAL_SHARD_SIZE = int('0xffffffffffffffffffffffffffffffff', base=16)
TOTAL_HASH_LENGTH = len('0xffffffffffffffffffffffffffffffff') - 2


def _split_one_shard_to_multiple(client, project, logstore, shard_info, count, current_shard_count):
    """return new_rw_shards_list, increased_shard_count """
    distance = shard_info['length'] // count
    if distance <= 0 or count <= 1:
        return [shard_info['info']], 0

    rw_shards, increased_shard_count = {shard_info['id']: shard_info['info']}, 0
    for x in range(1, count):
        new_hash = shard_info['start'] + distance * x
        new_hash = hex(new_hash)[2:].strip('lL')
        new_hash = '0' * (TOTAL_HASH_LENGTH - len(new_hash)) + new_hash
        try:
            if x == 1:
                res = client.split_shard(project, logstore, shard_info['id'], new_hash)
            else:
                res = client.split_shard(project, logstore, current_shard_count - 1, new_hash)

            # new rw_shards
            for shard in res.shards:
                if shard['status'] == 'readonly':
                    del rw_shards[shard['shardID']]
                else:
                    rw_shards[shard['shardID']] = shard

            current_shard_count += res.count - 1
            increased_shard_count += res.count - 1
            logger.info("split shard: project={0}, logstore={1}, shard_info={2}, count={3}, current_shard_count={4}".format(project, logstore, shard_info, count, current_shard_count))
        except Exception as ex:
            print(ex)
            print(x, project, logstore, shard_info, count, current_shard_count)
            raise

    return rw_shards.values(), increased_shard_count


def arrange_shard(client, project, logstore, count, stategy=None):
    total_length = TOTAL_SHARD_SIZE
    avg_len = total_length * 1.0 / count

    res = client.list_shards(project, logstore)
    current_shard_count = res.count
    rw_shards = [shard for shard in res.shards if shard['status'].lower() == 'readwrite']
    split_left = count - len(rw_shards)

    while split_left > 0:
        current_rw_shards = [{'id': shard['shardID'],
                              'start': int('0x' + shard['inclusiveBeginKey'], base=16),
                              'end': int('0x' + shard['exclusiveEndKey'], base=16),
                              'length': int('0x' + shard['exclusiveEndKey'], base=16)
                                        - int('0x' + shard['inclusiveBeginKey'], base=16),
                              'info': shard}
                             for shard in rw_shards]
        # need to split shard
        updated_rw_shards = []
        for i, shard in enumerate(sorted(current_rw_shards, key=lambda x: x['length'], reverse=True)):
            if split_left <= 0:  # no need to split any more
                break

            sp_cnt = int(shard['length'] // avg_len)
            if sp_cnt <= 1 and i == 0:  # cannot split, but be the first one, should split it
                sp_cnt = 2
            elif sp_cnt <= 0:
                sp_cnt = 1

            new_rw_shards, increased_shard_count = _split_one_shard_to_multiple(client, project, logstore, shard,
                                                                                sp_cnt, current_shard_count)
            updated_rw_shards.extend(new_rw_shards)
            current_shard_count += increased_shard_count
            split_left -= len(new_rw_shards) - 1

        # update current rw shards
        rw_shards = updated_rw_shards

    return ''


def _get_percentage(v, t, digit=2):
    return str(round(v * 100.0 / t, digit)) + '%'


class ResourceUsageResponse(LogResponse):
    def __init__(self, usage):
        LogResponse.__init__(self, {}, body=usage)


def get_resource_usage(client, project):
    result = {"logstore": {},
              "shard": {"logstores": {}},
              "logtail": {},
              "consumer_group": {"logstores": {}},
              }
    res = client.list_logstore(project, size=-1)
    result["logstore"] = {"count": {"status": res.total,
                                    "limitation": 200,
                                    "usage": _get_percentage(res.total, 200)}}
    shard_count = 0
    consumer_group_count = 0
    for logstore in res.logstores:
        res = client.list_shards(project, logstore)
        shard_count += res.count
        result["shard"]["logstores"][logstore] = {"status": res.count}

        res = client.list_consumer_group(project, logstore)
        if res.count:
            result["consumer_group"]["logstores"][logstore] = {"status": res.count, "limitation": 10,
                                                               "usage": _get_percentage(res.count, 10)}
        consumer_group_count += res.count

    result["shard"]["count"] = {"status": shard_count, "limitation": 200, "usage": _get_percentage(shard_count, 200)}
    result["consumer_group"]["count"] = {"status": consumer_group_count}

    res = client.list_logtail_config(project, offset=1000)  # pass 1000 to just get total count
    result["logtail"] = {"count": {"status": res.total,
                                   "limitation": 100,
                                   "usage": _get_percentage(res.total, 100)}}
    res = client.list_machine_group(project, offset=1000)  # pass 1000 to just get total count
    result["machine_group"] = {"count": {"status": res.total,
                                         "limitation": 100,
                                         "usage": _get_percentage(res.total, 100)}}
    res = client.list_savedsearch(project, offset=1000)  # pass 1000 to just get total count
    result["saved_search"] = {"count": {"status": res.total,
                                        "limitation": 100,
                                        "usage": _get_percentage(res.total, 100)}}

    res = client.list_dashboard(project, offset=1000)  # pass 1000 to just get total count
    result["dashboard"] = {"count": {"status": res.total,
                                     "limitation": 50,
                                     "usage": _get_percentage(res.total, 50)}}

    return ResourceUsageResponse(result)


def put_logs_auto_div(client, req, div=1):
    try:
        count = len(req.logitems)
        p1 = count // 2

        if div > 1 and p1 > 0:
            # divide req into multiple ones
            req1 = PutLogsRequest(project=req.project, logstore=req.logstore, topic=req.topic, source=req.source,
                                  logitems=req.logitems[:p1],
                                  hashKey=req.hashkey, compress=req.compress, logtags=req.logtags)
            req2 = PutLogsRequest(project=req.project, logstore=req.logstore, topic=req.topic, source=req.source,
                                  logitems=req.logitems[p1:],
                                  hashKey=req.hashkey, compress=req.compress, logtags=req.logtags)
            res = put_logs_auto_div(client, req1)
            return put_logs_auto_div(client, req2)
        else:
            return client.put_logs(req)
    except LogException as ex:
        if ex.get_error_code() == 'InvalidLogSize':
            return put_logs_auto_div(client, req, div=2)
        raise ex


def _transform_events_to_logstore(runner, events, to_client, to_project, to_logstore):
    count = removed = processed = failed = 0
    new_events = defaultdict(list)

    default_time = time.time()
    for src_event in events:
        count += 1
        new_event = runner(src_event)

        if new_event is None:
            removed += 1
            continue

        if not isinstance(new_event, (tuple, list)):
            new_event = (new_event, )

        for event in new_event:
            if not isinstance(event, dict):
                logger.error("transform_data: get unknown type of processed event: {0}".format(event))
                continue

            dt = int(event.get('__time__', default_time)) // 60  # group logs in same minute
            topic = ''
            source = ''
            if "__topic__" in event:
                topic = event['__topic__']
                del event["__topic__"]
            if "__source__" in event:
                source = event['__source__']
                del event["__source__"]

            new_events[(dt, topic, source)].append(event)

    for (dt, topic, source), contents in six.iteritems(new_events):

        items = []

        for content in contents:
            st = content.get("__time__", default_time)
            if "__time__" in content:
                del content['__time__']

            ct = list(six.iteritems(content))
            item = LogItem(st, ct)
            items.append(item)

        req = PutLogsRequest(project=to_project, logstore=to_logstore, topic=topic, source=source, logitems=items)
        res = put_logs_auto_div(to_client, req)
        processed += len(items)

    return count, removed, processed, failed


def transform_worker(from_client, from_project, from_logstore, shard_id, from_time, to_time,
                     config,
                     to_client, to_project, to_logstore, batch_size=None, compress=None,
                     ):
    next_cursor = "As from_time configured"
    try:
        runner = Runner(config)
        iter_data = from_client.pull_log(from_project, from_logstore, shard_id, from_time, to_time, batch_size=batch_size,
                                         compress=compress)

        count = removed = processed = failed = 0
        for s in iter_data:
            events = s.get_flatten_logs_json_auto()

            c, r, p, f = _transform_events_to_logstore(runner, events, to_client, to_project, to_logstore)
            count += c
            removed += r
            processed += p
            failed += f

            next_cursor = s.next_cursor
        return shard_id, count, removed, processed, failed
    except Exception as ex:
        logger.error("transform data failed: task info {0} failed to copy data to target, next cursor: {1} detail: {2}".
                     format(
            (from_project, from_logstore, shard_id, from_time, to_time, to_client, to_project, to_logstore),
            next_cursor, ex), exc_info=True)
        raise


class TransformDataConsumer(ConsumerProcessorBase):
    runner = None
    to_client = None
    to_project = None
    to_logstore = None

    def __init__(self, status_updator):
        super(TransformDataConsumer, self).__init__()
        self.count = self.removed = self.processed = self.failed = 0
        self.status_updator = status_updator

    @staticmethod
    def set_transform_options(config, to_client, to_project, to_logstore):
        TransformDataConsumer.runner = Runner(config)
        TransformDataConsumer.to_client = to_client
        TransformDataConsumer.to_project = to_project
        TransformDataConsumer.to_logstore = to_logstore

    def process(self, log_groups, check_point_tracker):
        logger.info("TransformDataConsumer::process: get log groups")

        logs = PullLogResponse.loggroups_to_flattern_list(log_groups)
        c, r, p, f = _transform_events_to_logstore(self.runner, logs, self.to_client, self.to_project, self.to_logstore)

        self.count += c
        self.removed += r
        self.processed += p
        self.failed += f

        # save check point
        self.save_checkpoint(check_point_tracker)

    def shutdown(self, check_point_tracker):
        super(TransformDataConsumer, self).shutdown(check_point_tracker)
        self.status_updator(self.shard_id, self.count, self.removed, self.processed, self.failed)


def transform_data(from_client, from_project, from_logstore, from_time,
                   to_time=None,
                   to_client=None, to_project=None, to_logstore=None,
                   shard_list=None,
                   config=None,
                   batch_size=None, compress=None,
                   cg_name=None, c_name=None,
                   cg_heartbeat_interval=None, cg_data_fetch_interval=None, cg_in_order=None,
                   cg_worker_pool_size=None
                   ):
    """
    transform data from one logstore to another one (could be the same or in different region), the time is log received time on server side.

    """
    if not config:
        logger.info("transform_data: config is not configured, use copy data by default.")
        return copy_data(from_client, from_project, from_logstore, from_time, to_time=to_time,
                         to_client=to_client, to_project=to_project, to_logstore=to_logstore,
                         shard_list=shard_list,
                         batch_size=batch_size, compress=compress)

    to_client = to_client or from_client

    # increase the timeout to 2 min at least
    from_client.timeout = max(from_client.timeout, 120)
    to_client.timeout = max(to_client.timeout, 120)
    to_project = to_project or from_project
    to_logstore = to_logstore or from_logstore

    if not cg_name:
        # batch mode
        to_time = to_time or "end"
        cpu_count = multiprocessing.cpu_count() * 2
        shards = from_client.list_shards(from_project, from_logstore).get_shards_info()
        current_shards = [str(shard['shardID']) for shard in shards]
        target_shards = _parse_shard_list(shard_list, current_shards)

        worker_size = min(cpu_count, len(target_shards))

        result = dict()
        total_count = 0
        total_removed = 0
        with ProcessPoolExecutor(max_workers=worker_size) as pool:
            futures = [pool.submit(transform_worker, from_client, from_project, from_logstore, shard,
                                   from_time, to_time, config,
                                   to_client, to_project, to_logstore,
                                   batch_size=batch_size, compress=compress)
                       for shard in target_shards]

            for future in as_completed(futures):
                if future.exception():
                    logger.error("get error when transforming data: {0}".format(future.exception()))
                else:
                    partition, count, removed, processed, failed = future.result()
                    total_count += count
                    total_removed += removed
                    if count:
                        result[partition] = {"total_count": count, "transformed":
                            processed, "removed": removed, "failed": failed}

        return LogResponse({}, {"total_count": total_count, "shards": result})

    else:
        # consumer group mode
        c_name = c_name or "transform_data_{0}".format(multiprocessing.current_process().pid)
        cg_heartbeat_interval = cg_heartbeat_interval or 20
        cg_data_fetch_interval = cg_data_fetch_interval or 2
        cg_in_order = False if cg_in_order is None else cg_in_order
        cg_worker_pool_size = cg_worker_pool_size or 3

        option = LogHubConfig(from_client._endpoint, from_client._accessKeyId, from_client._accessKey,
                              from_project, from_logstore, cg_name,
                              c_name, cursor_position=CursorPosition.SPECIAL_TIMER_CURSOR,
                              cursor_start_time=from_time,
                              cursor_end_time=to_time,
                              heartbeat_interval=cg_heartbeat_interval, data_fetch_interval=cg_data_fetch_interval,
                              in_order=cg_in_order,
                              worker_pool_size=cg_worker_pool_size)

        TransformDataConsumer.set_transform_options(config, to_client, to_project, to_logstore)

        result = {"total_count": 0, "shards": {}}
        l = RLock()

        def status_updator(shard_id, count=0, removed=0, processed=0, failed=0):
            logger.info("status update is called, shard: {0}, count: {1}, removed: {2}, processed: {3}, failed: {4}".format(shard_id, count, removed, processed, failed))

            with l:
                result["total_count"] += count
                if shard_id in result["shards"]:
                    data = result["shards"][shard_id]
                    result["shards"][shard_id] = {"total_count": data["total_count"] + count, "transformed": data["transformed"] + processed, "removed": data["removed"] + removed, "failed": data["failed"] + failed}
                else:
                    result["shards"][shard_id] = {"total_count": count, "transformed": processed, "removed": removed, "failed": failed}

        worker = ConsumerWorker(TransformDataConsumer, consumer_option=option, args=(status_updator, ) )
        worker.start()

        try:
            while worker.is_alive():
                worker.join(timeout=60)
            logger.info("transform_data: worker exit unexpected, try to shutdown it")
            worker.shutdown()
        except KeyboardInterrupt:
            logger.info("transform_data: *** try to exit **** ")
            print("try to stop transforming data.")
            worker.shutdown()
            worker.join(timeout=120)

        return LogResponse({}, result)
