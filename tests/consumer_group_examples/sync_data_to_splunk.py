# -*- coding: utf-8 -*-

import os
import logging
from logging.handlers import RotatingFileHandler
from aliyun.log.consumer import *
from aliyun.log.pulllog_response import PullLogResponse
from multiprocessing import current_process
import json
import socket
import requests

# configure logging file
root = logging.getLogger()
handler = RotatingFileHandler("{0}_{1}.log".format(os.path.basename(__file__), current_process().pid), maxBytes=100*1024*1024, backupCount=5)
handler.setFormatter(logging.Formatter(fmt='[%(asctime)s] - [%(threadName)s] - {%(module)s:%(funcName)s:%(lineno)d} %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
root.setLevel(logging.INFO)
root.addHandler(handler)
root.addHandler(logging.StreamHandler())

logger = logging.getLogger(__name__)


class SyncData(ConsumerProcessorBase):
    """
    this consumer will forward logs to Splunk.
    """
    def __init__(self, splunk_setting=None):
        """
        :param splunk_setting:
            {
                "host": "10.1.2.3",
                "port": 80,
                "token": "a023nsdu123123123",
                'https': True,              # optional, bool
                'timeout': 120,             # optional, int
                'ssl_verify': True,         # optional, bool
                "sourcetype": "",            # optional, sourcetype
                "index": "",                # optional, string
                "source": "",               # optional, string
            }
        """

        super(SyncData, self).__init__()   # remember to call base's init

        assert splunk_setting, ValueError("You need to configure settings of remote target")
        assert isinstance(splunk_setting, dict), ValueError("The settings should be dict to include necessary address and confidentials.")

        self.option = splunk_setting
        self.timeout = self.option.get("timeout", 120)

        # Testing connectivity
        s = socket.socket()
        s.settimeout(self.timeout)
        s.connect((self.option["host"], self.option['port']))

        self.r = requests.session()
        self.r.max_redirects = 1
        self.r.verify = self.option.get("ssl_verify", True)
        self.r.headers['Authorization'] = "Splunk {}".format(self.option['token'])
        self.url = "{0}://{1}:{2}/services/collector".format("http" if not self.option.get('https') else "https", self.option['host'], self.option['port'])

        self.default_fields = {}
        if self.option.get("sourcetype"):
            self.default_fields['sourcetype'] = self.option.get("sourcetype")
        if self.option.get("source"):
            self.default_fields['source'] = self.option.get("source")
        if self.option.get("index"):
            self.default_fields['index'] = self.option.get("index")

    def process(self, log_groups, check_point_tracker):
        logs = PullLogResponse.loggroups_to_flattern_list(log_groups, time_as_str=True, decode_bytes=True)
        logger.info("Get data from shard {0}, log count: {1}".format(self.shard_id, len(logs)))
        for log in logs:
            # Put your sync code here to send to remote.
            # the format of log is just a dict with example as below (Note, all strings are unicode):
            #    Python2: {u"__time__": u"12312312", u"__topic__": u"topic", u"field1": u"value1", u"field2": u"value2"}
            #    Python3: {"__time__": "12312312", "__topic__": "topic", "field1": "value1", "field2": "value2"}
            event = {}
            event.update(self.default_fields)
            # suppose we only care about audit log
            event['time'] = log[u'__time__']
            event['fields'] = {}
            del log['__time__']
            event['fields'].update(log)

            data = json.dumps(event, sort_keys=True)

            try:
                req = self.r.post(self.url, data=data, timeout=self.timeout)
                req.raise_for_status()
            except Exception as err:
                logger.debug("Failed to connect to remote Splunk server ({0}). Exception: {1}".format(self.url, err))
                raise err

                # TODO: add some error handling here or retry etc.

        logger.info("Complete send data to remote")

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
    # Could be "begin", "end", "specific time format in ISO", it's log receiving time.
    cursor_start_time = "2018-12-26 0:0:0"

    # once a client doesn't report to server * heartbeat_interval * 2 interval, server will consider it's offline and re-assign its task to another consumer.
    # thus  don't set the heatbeat interval too small when the network badwidth or performance of consumtion is not so good.
    heartbeat_interval = 20

    # if the coming data source data is not so frequent, please don't configure it too small (<1s)
    data_fetch_interval = 1

    # create one consumer in the consumer group
    option = LogHubConfig(endpoint, accessKeyId, accessKey, project, logstore, consumer_group, consumer_name,
                          cursor_position=CursorPosition.SPECIAL_TIMER_CURSOR,
                          cursor_start_time=cursor_start_time,
                          heartbeat_interval=heartbeat_interval,
                          data_fetch_interval=data_fetch_interval)

    # monitor options
    settings = {
                "host": "10.1.2.3",
                "port": 80,
                "token": "a023nsdu123123123",
                'https': False,              # optional, bool
                'timeout': 120,             # optional, int
                'ssl_verify': True,         # optional, bool
                "sourcetype": "",            # optional, sourcetype
                "index": "",                # optional, index
                "source": "",               # optional, source
            }

    return option, settings


def main():
    option, settings = get_monitor_option()

    logger.info("*** start to consume data...")
    worker = ConsumerWorker(SyncData, option, args=(settings,) )
    worker.start(join=True)


if __name__ == '__main__':
    main()
