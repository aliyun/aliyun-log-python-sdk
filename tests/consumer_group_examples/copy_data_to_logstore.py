# -*- coding: utf-8 -*-

import os
import logging
from logging.handlers import RotatingFileHandler
from aliyun.log.consumer import *
from aliyun.log import LogClient
from aliyun.log.pulllog_response import PullLogResponse
from multiprocessing import current_process
from functools import partial

# configure logging file
root = logging.getLogger()
handler = RotatingFileHandler("{0}_{1}.log".format(os.path.basename(__file__), current_process().pid), maxBytes=100*1024*1024, backupCount=5)
handler.setFormatter(logging.Formatter(fmt='[%(asctime)s] - [%(threadName)s] - {%(module)s:%(funcName)s:%(lineno)d} %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
root.setLevel(logging.INFO)
root.addHandler(handler)
root.addHandler(logging.StreamHandler())

logger = logging.getLogger(__name__)


def get_copy_option():
    ##########################
    # Basic options
    ##########################

    # load connection info env and consumer group name from envs
    accessKeyId = os.environ.get('SLS_AK_ID', '')
    accessKey = os.environ.get('SLS_AK_KEY', '')
    endpoint = os.environ.get('SLS_ENDPOINT', '')
    project = os.environ.get('SLS_PROJECT', '')
    logstore = os.environ.get('SLS_LOGSTORE', '')
    to_endpoint = os.environ.get('SLS_ENDPOINT_TO', endpoint)
    to_project = os.environ.get('SLS_PROJECT_TO', project)
    to_logstore = os.environ.get('SLS_LOGSTORE_TO', '')
    consumer_group = os.environ.get('SLS_CG', '')

    assert endpoint and accessKeyId and accessKey and project and logstore and consumer_group, \
        ValueError("endpoint/access_id/key/project/logstore/consumer_group/name cannot be empty")

    assert to_endpoint and to_project and to_logstore, ValueError("to endpoint/to project/to logstore cannot be empty")

    ##########################
    # Some advanced options
    ##########################

    # DON'T configure the consumer name especially when you need to run this program in parallel
    consumer_name = "{0}-{1}".format(consumer_group, current_process().pid)

    # copy from the latest one.
    option = LogHubConfig(endpoint, accessKeyId, accessKey, project, logstore, consumer_group, consumer_name,
                          cursor_position=CursorPosition.END_CURSOR)

    # bind put_log_raw which is faster
    to_client = LogClient(to_endpoint, accessKeyId, accessKey)
    put_method = partial(to_client.put_log_raw, project=to_project, logstore=to_logstore)

    return option, put_method


if __name__ == '__main__':
    option, put_method = get_copy_option()

    def copy_data(shard_id, log_groups):
        log_count = PullLogResponse.get_log_count_from_group(log_groups)
        logger.info("Get data from shard {0}, log count: {1}".format(shard_id, log_count))

        for log_group in log_groups.LogGroups:
            # update topic
            log_group.Topic += "_copied"
            put_method(log_group=log_group)

    logger.info("*** start to consume data...")
    worker = ConsumerWorker(ConsumerProcessorAdaptor, option, args=(copy_data, ))
    worker.start(join=True)
