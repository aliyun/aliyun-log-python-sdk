# -*- coding: utf-8 -*-

import os
import time

from aliyun.log.consumer import *
from aliyun.log import *
from threading import RLock


class SampleConsumer(ConsumerProcessorBase):
    shard_id = -1
    last_check_time = 0
    log_results = []
    lock = RLock()

    def initialize(self, shard):
        self.shard_id = shard

    def process(self, log_groups, check_point_tracker):
        for log_group in log_groups.LogGroups:
            items = []
            for log in log_group.Logs:
                item = dict()
                item['time'] = log.Time
                for content in log.Contents:
                    item[content.Key] = content.Value
                items.append(item)
            log_items = dict()
            log_items['topic'] = log_group.Topic
            log_items['source'] = log_group.Source
            log_items['logs'] = items

            with SampleConsumer.lock:
                SampleConsumer.log_results.append(log_items)
                print(log_items)

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

        # None means succesful process
        # if need to roll-back to previous checkpoint，return check_point_tracker.get_check_point()
        return None

    def shutdown(self, check_point_tracker):
        try:
            check_point_tracker.save_check_point(True)
        except Exception:
            import traceback
            traceback.print_exc()


test_item_count = 20


def _prepare_data(client, project, logstore):
    topic = ''
    source = ''

    for i in range(0, test_item_count):
        logitemList = []  # LogItem list

        contents = [
            ('user', 'magic_user_' + str(i)),
            ('avg', 'magic_age_' + str(i))
        ]
        logItem = LogItem()
        logItem.set_time(int(time.time()))
        logItem.set_contents(contents)

        logitemList.append(logItem)

        request = PutLogsRequest(project, logstore, topic, source, logitemList)

        response = client.put_logs(request)
        response.log_print()


def sleep_until(seconds, exit_condition=None, expect_error=False):
    if not exit_condition:
        time.sleep(seconds)
        return

    s = time.time()
    while time.time() - s < seconds:
        try:
            if exit_condition():
                break
        except Exception:
            if expect_error:
                continue
        time.sleep(1)


def sample_consumer_group():
    # load options from envs
    endpoint = os.environ.get('ALIYUN_LOG_SAMPLE_ENDPOINT', '')
    accessKeyId = os.environ.get('ALIYUN_LOG_SAMPLE_ACCESSID', '')
    accessKey = os.environ.get('ALIYUN_LOG_SAMPLE_ACCESSKEY', '')
    project = os.environ.get('ALIYUN_LOG_SAMPLE_PROJECT', '')
    logstore = ''
    consumer_group = 'consumer-group-1'
    consumer_name1 = "consumer-group-1-A"
    consumer_name2 = "consumer-group-1-B"
    token = ""

    if not logstore:
        logstore = 'consumer_group_test_' + str(time.time()).replace('.', '_')

    assert endpoint and accessKeyId and accessKey and project, ValueError("endpoint/access_id/key and "
                                                                          "project cannot be empty")

    # prepare clients
    client = LogClient(endpoint, accessKeyId, accessKey, token)
    ret = client.create_logstore(project, logstore, 2, 4)
    ret.log_print()

    time.sleep(60)
    SampleConsumer.log_results = []

    try:
        # prepare data
        _prepare_data(client, project, logstore)

        # create two consumers in the consumer group
        option1 = LogHubConfig(endpoint, accessKeyId, accessKey, project, logstore, consumer_group,
                               consumer_name1, cursor_position=CursorPosition.BEGIN_CURSOR, heartbeat_interval=6,
                               data_fetch_interval=1)
        option2 = LogHubConfig(endpoint, accessKeyId, accessKey, project, logstore, consumer_group,
                               consumer_name2, cursor_position=CursorPosition.BEGIN_CURSOR, heartbeat_interval=6,
                               data_fetch_interval=1)

        print("*** start to consume data...")
        client_worker1 = ConsumerWorker(SampleConsumer, consumer_option=option1)
        client_worker1.start()
        client_worker2 = ConsumerWorker(SampleConsumer, consumer_option=option2)
        client_worker2.start()

        sleep_until(120, lambda: len(SampleConsumer.log_results) >= test_item_count)

        print("*** consumer group status ***")
        ret = client.list_consumer_group(project, logstore)
        ret.log_print()

        for c in ret.get_consumer_groups():
            ret = client.get_check_point_fixed(project, logstore, c.get_consumer_group_name())
            ret.log_print()

        print("*** stopping workers")
        client_worker1.shutdown()
        client_worker2.shutdown()

    finally:
        # clean-up
        ret = client.delete_logstore(project, logstore)
        ret.log_print()

    # validate
    ret = str(SampleConsumer.log_results)
    print("*** get content:")
    print(ret)

    assert 'magic_user_0' in ret and 'magic_age_0' in ret \
           and 'magic_user_' + str(test_item_count-1) in ret \
           and 'magic_age_' + str(test_item_count-1) in ret


if __name__ == '__main__':
    sample_consumer_group()
