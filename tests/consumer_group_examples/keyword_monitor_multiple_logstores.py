# -*- coding: utf-8 -*-

import os
import logging
from logging.handlers import RotatingFileHandler
from aliyun.log.consumer import *
from aliyun.log.pulllog_response import PullLogResponse
from multiprocessing import current_process
import re
from concurrent.futures import ThreadPoolExecutor

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
    def __init__(self, keywords=None, logstore=None):
        super(KeywordMonitor, self).__init__()  # remember to call base init

        assert keywords, ValueError("At least you need to configure one keywords to monitor")
        assert isinstance(keywords, dict), ValueError("The keyword should be dict as in field:keyword format.")
        self.keywords = keywords
        self.kw_check = {}
        for k, v in self.keywords.items():
            self.kw_check[k] = re.compile(v)
        self.logstore = logstore

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
    endpoints = os.environ.get('SLS_ENDPOINTS', '').split(";")  # endpoints list separated by ;
    accessKeyId = os.environ.get('SLS_AK_ID', '')
    accessKey = os.environ.get('SLS_AK_KEY', '')
    projects = os.environ.get('SLS_PROJECTS', '').split(";")    # projects list, separated by ; must be same len as endpoints
    logstores = os.environ.get('SLS_LOGSTORES', '').split(";")  # logstores list, seperated by ; pared with project. and separated by ',' for one project.
    consumer_group = os.environ.get('SLS_CG', '')

    assert endpoints and accessKeyId and accessKey and projects and logstores and consumer_group, \
        ValueError("endpoints/access_id/key/projects/logstores/consumer_group/name cannot be empty")

    assert len(endpoints) == len(projects) == len(logstores), ValueError("endpoints/projects/logstores must be paired")

    ##########################
    # Some advanced options
    ##########################

    # DON'T configure the consumer name especially when you need to run this program in parallel
    consumer_name = "{0}-{1}".format(consumer_group, current_process().pid)

    # This options is used for initialization, will be ignored once consumer group is created and each shard has beeen started to be consumed.
    # Could be "begin", "end", "specific time format in ISO", it's log receiving time.
    cursor_start_time = "begin"

    exeuctor = ThreadPoolExecutor(max_workers=2)

    options = []
    for i in range(len(endpoints)):
        endpoint = endpoints[i].strip()
        project = projects[i].strip()
        if not endpoint or not project:
            logger.error("project: {0} or endpoint {1} is empty, skip".format(project, endpoint))
            continue

        logstore_list = logstores[i].split(",")
        for logstore in logstore_list:
            logstore = logstore.strip()
            if not logstore:
                logger.error("logstore for project: {0} or endpoint {1} is empty, skip".format(project, endpoint))
                continue

            option = LogHubConfig(endpoint, accessKeyId, accessKey, project, logstore, consumer_group,
                                  consumer_name, cursor_position=CursorPosition.SPECIAL_TIMER_CURSOR,
                                  cursor_start_time=cursor_start_time, shared_executor=exeuctor)
            options.append(option)

    # monitor options
    keywords = {'status': r'5\d{2}'}

    return exeuctor, options, keywords


def main():
    exeuctor, options, keywords = get_monitor_option()

    logger.info("*** start to consume data...")
    workers = []

    for option in options:
        worker = ConsumerWorker(KeywordMonitor, option, args=(keywords,) )
        workers.append(worker)
        worker.start()

    try:
        for i, worker in enumerate(workers):
            while worker.is_alive():
                worker.join(timeout=60)
            logger.info("worker project: {0} logstore: {1} exit unexpected, try to shutdown it".format(
                options[i].project, options[i].logstore))
            worker.shutdown()
    except KeyboardInterrupt:
        logger.info("*** try to exit **** ")
        for worker in workers:
            worker.shutdown()

        # wait for all workers to shutdown before shutting down executor
        for worker in workers:
            while worker.is_alive():
                worker.join(timeout=60)

    exeuctor.shutdown()


if __name__ == '__main__':
    main()
