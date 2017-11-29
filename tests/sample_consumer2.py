# -*- coding: utf-8 -*-

import os
import time

from aliyun.log.consumer import *
from aliyun.log import *
from threading import RLock
from datetime import datetime
import tzlocal
import sys

lock = RLock()

get_i = 0
get_total = 0
put_i = 0


class SampleConsumer(ConsumerProcessorBase):
    shard_id = -1
    last_check_time = 0

    def initialize(self, shard):
        self.shard_id = shard

    def process(self, log_groups, check_point_tracker):
        global get_i, get_total
        count = len(log_groups.LogGroups)
        get_i += 1
        get_total += count
        with lock:
            print("<<< Get log_items({0}): {1} logs, total: {2} logs".format(get_i, count, get_total))

        current_time = time.time()
        if current_time - self.last_check_time > 3:
            try:
                self.last_check_time = current_time
                check_point_tracker.save_check_point(True)
            except Exception:
                import traceback
                traceback.print_exc()
        else:
            try:
                check_point_tracker.save_check_point(False)
            except Exception:
                import traceback
                traceback.print_exc()

        return None

    def shutdown(self, check_point_tracker):
        try:
            check_point_tracker.save_check_point(True)
        except Exception:
            import traceback
            traceback.print_exc()


def prepare_test():
    # load options from envs
    endpoint = os.environ.get('ALIYUN_LOG_SAMPLE_ENDPOINT', '')
    accessKeyId = os.environ.get('ALIYUN_LOG_SAMPLE_ACCESSID', '')
    accessKey = os.environ.get('ALIYUN_LOG_SAMPLE_ACCESSKEY', '')
    project = os.environ.get('ALIYUN_LOG_SAMPLE_PROJECT', '')
    logstore = os.environ.get('ALIYUN_LOG_SAMPLE_LOGSTORE', '')
    consumer_group = 'consumer-group-1'
    consumer_name1 = "consumer-group-1-A"
    token = ""

    if not logstore:
        logstore = 'consumer_group_test_' + str(time.time()).replace('.', '_')

    print("** logstore: ", logstore)
    assert endpoint and accessKeyId and accessKey and project and logstore, ValueError("endpoint/access_id/key and "
                                                                          "project/logstore cannot be empty")

    # prepare clients
    client = LogClient(endpoint, accessKeyId, accessKey, token)

    # create one consumer in the consumer group
    option = LogHubConfig(endpoint, accessKeyId, accessKey, project, logstore, consumer_group,
                           consumer_name1, cursor_position=CursorPosition.BEGIN_CURSOR, heartbeat_interval=6,
                           data_fetch_interval=1)

    return client, option, project, logstore, consumer_group


def write_data(client, project, logstore):
    global put_i
    put_i += 1
    topic = ''
    source = ''

    logitemList = []  # LogItem list

    contents = [
        ('user', 'magic_user_' + str(put_i)),
        ('avg', 'magic_age_' + str(put_i))
    ]
    logItem = LogItem()
    logItem.set_time(int(time.time()))
    logItem.set_contents(contents)

    logitemList.append(logItem)

    request = PutLogsRequest(project, logstore, topic, source, logitemList)

    response = client.put_logs(request)
    with lock:
        print(">>> Put logs: {0}".format(put_i))


def show_consumer_group():
    client, option, project, logstore, consumer_group = prepare_test()

    local_timezone = tzlocal.get_localzone()

    try:
        while True:
            ret = client.get_check_point_fixed(project, logstore, consumer_group)

            with lock:
                # ret.log_print()
                print("***** consumer group status*****")
                if not ret.consumer_group_check_poins:
                    print("[]")
                else:
                    print("***consumer\t\t\tshard\tcursor time\t\t\t\t\tupdate time")
                    for status in ret.consumer_group_check_poins:
                        update_time = datetime.fromtimestamp(status['updateTime']/1000000, local_timezone)
                        cursor_time = datetime.fromtimestamp(status.get('checkpoint_previous_cursor_time', 0),
                                                             local_timezone)
                        print("{0}\t{1}\t\t{2}\t{3}".format(status["consumer"], status['shard'],
                                                          cursor_time, update_time))

            time.sleep(1)
    except KeyboardInterrupt:
        print("***** exit *****")


def produce_consumer_group():
    client, option, project, logstore, consumer_group= prepare_test()

    try:
        while True:
            write_data(client, project, logstore)
            time.sleep(1)
    except KeyboardInterrupt:
        print("*** exit **** ")


def consume_consumer_group():
    client, option, project, logstore, consumer_group= prepare_test()

    print("*** start to consume data...")
    client_worker1 = ConsumerWorker(SampleConsumer, consumer_option=option)
    client_worker1.start()

    try:
        while True:
            time.sleep(100)
    except KeyboardInterrupt:
        print("*** try to exit **** ")

        client_worker1.shutdown()
        time.sleep(20)

    print("***** exit *****")


if __name__ == '__main__':
    cmd = sys.argv[1].lower() if sys.argv[1:] else ''

    if cmd == 'consume':
        consume_consumer_group()
    elif cmd == 'produce':
        produce_consumer_group()
    elif cmd == 'status':
        show_consumer_group()
    else:
        print("""Usage: consume|produce|status""")
