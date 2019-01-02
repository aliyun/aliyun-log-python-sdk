# -*- coding: utf-8 -*-

import os
import logging
from logging.handlers import RotatingFileHandler
from aliyun.log.consumer import *
from aliyun.log.pulllog_response import PullLogResponse
from multiprocessing import current_process
import re

# configure logging file
root = logging.getLogger()
handler = RotatingFileHandler("{0}_{1}.log".format(os.path.basename(__file__), current_process().pid), maxBytes=100*1024*1024, backupCount=5)
handler.setFormatter(logging.Formatter(fmt='[%(asctime)s] - [%(threadName)s] - {%(module)s:%(funcName)s:%(lineno)d} %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
root.setLevel(logging.INFO)
root.addHandler(handler)
root.addHandler(logging.StreamHandler())

logger = logging.getLogger(__name__)


class KeywordMonitor(ConsumerProcessorBase):
    """
    this consumer will keep monitor with k-v fields. like {"content": "error"}
    """
    def __init__(self, keywords=None):
        super(KeywordMonitor, self).__init__()  # remember to call base init

        assert keywords, ValueError("At least you need to configure one keywords to monitor")
        assert isinstance(keywords, dict), ValueError("The keyword should be dict as in field:keyword format.")
        self.keywords = keywords
        self.kw_check = {}
        for k, v in self.keywords.items():
            self.kw_check[k] = re.compile(v)

    def process(self, log_groups, check_point_tracker):
        logs = PullLogResponse.loggroups_to_flattern_list(log_groups)
        logger.info("Get data from shard {0}, log count: {1}".format(self.shard_id, len(logs)))
        match_count = 0
        sample_error_log = ""
        for log in logs:
            m = None
            for k, c in self.kw_check.items():
                if k in log:
                    m = c.search(log[k])
                    if m:
                        logger.debug('Keyword detected for shard "{0}" with keyword: "{1}" in field "{2}", log: {3}'
                                    .format(self.shard_id, log[k], k, log))
            if m:
                match_count += 1
                sample_error_log = log

        if match_count:
            logger.info("Keyword detected for shard {0}, count: {1}, example: {2}".format(self.shard_id, match_count, sample_error_log))
        else:
            logger.debug("No keyword detected for shard {0}".format(self.shard_id))

        self.save_checkpoint(check_point_tracker)


def get_monitor_option():
    ##########################
    # Basic options
    ##########################

    # load connection info env and consumer group name from envs
    endpoint = os.environ.get('SLS_ENDPOINT', '')
    accessKeyId = os.environ.get('SLS_AK_ID', '')
    accessKey = os.environ.get('SLS_AK_KEY', '')
    project = os.environ.get('SLS_PROJECT', '')
    logstore = os.environ.get('SLS_LOGSTORE', '')
    consumer_group = os.environ.get('SLS_CG', '')

    assert endpoint and accessKeyId and accessKey and project and logstore and consumer_group, \
        ValueError("endpoint/access_id/key/project/logstore/consumer_group/name cannot be empty")

    ##########################
    # Some advanced options
    ##########################

    # DON'T configure the consumer name especially when you need to run this program in parallel
    consumer_name = "{0}-{1}".format(consumer_group, current_process().pid)

    # This options is used for initialization, will be ignored once consumer group is created and each shard has beeen started to be consumed.
    cursor_position = CursorPosition.END_CURSOR
    cursor_start_time = -1    # Will be used when cursor_position when could be "begin", "end", "specific time format in ISO", it's log receiving time.

    # during consuption, when shard is splitted, if need to consume the newly splitted shard after its parent shard (read-only) is finished consumption or not.
    # suggest keep it as False (don't care) until you have good reasion for it.
    in_order = False

    # once a client doesn't report to server * heartbeat_interval * 2 interval, server will consider it's offline and re-assign its task to another consumer.
    # thus  don't set the heatbeat interval too small when the network badwidth or performance of consumtion is not so good.
    heartbeat_interval = 20

    # if the coming data source data is not so frequent, please don't configure it too small (<1s)
    data_fetch_interval = 0.1

    # fetch size in each request, normally use default.
    # maximum is 1000, could be lower.
    # the lower the size the memory efficiency might be better.
    max_fetch_log_group_size = 1000

    # suggest keep the default size (2), use multiple process instead
    # when you need to have more concurrent processing, launch this consumer for mulitple times and give them different consuer name in same consumer group
    worker_pool_size = 2

    # create one consumer in the consumer group
    option = LogHubConfig(endpoint, accessKeyId, accessKey, project, logstore, consumer_group, consumer_name,
                          cursor_position=cursor_position,
                          cursor_start_time=cursor_start_time,
                          in_order=in_order,
                          heartbeat_interval=heartbeat_interval,
                          data_fetch_interval=data_fetch_interval,
                          max_fetch_log_group_size=max_fetch_log_group_size,
                          worker_pool_size=worker_pool_size)

    # monitor options
    keywords = {'status': r'5\d{2}'}

    return option, keywords


def main():
    option, keywords = get_monitor_option()

    logger.info("*** start to consume data...")
    worker = ConsumerWorker(KeywordMonitor, option, args=(keywords,) )
    worker.start(join=True)


if __name__ == '__main__':
    main()
