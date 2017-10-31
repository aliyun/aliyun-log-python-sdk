# -*- coding: utf-8 -*-

import os
import random
import time

from aliyun.log.consumer import ConsumerProcessorBase
from aliyun.log.consumer.config import LogHubConfig, CursorPosition
from aliyun.log.consumer.worker import ConsumerWorker
from aliyun.log.logclient import LogClient


class SampleConsumer(ConsumerProcessorBase):
    shard_id = -1
    last_check_time = 0

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
            print(log_items)
        current_time = time.time()
        if current_time - self.last_check_time > 3:
            try:
                check_point_tracker.save_check_point(True)
            except:
                import traceback
                traceback.print_exc()
        else:
            try:
                check_point_tracker.save_check_point(False)
            except:
                import traceback
                traceback.print_exc()

        # None means succesful process
        # if need to roll-back to previous checkpoint，return check_point_tracker.get_check_point()
        return None

    def shutdown(self, check_point_tracker):
        try:
            check_point_tracker.save_check_point(True)
        except Exception as e:
            import traceback
            traceback.print_exc()


endpoint = os.environ.get('ALIYUN_LOG_SAMPLE_ENDPOINT', 'ap-southeast-1.log.aliyuncs.com')
accessKeyId = os.environ.get('ALIYUN_LOG_SAMPLE_ACCESSID', '')
accessKey = os.environ.get('ALIYUN_LOG_SAMPLE_ACCESSKEY', '')
project = 'dlq-test-consumer'
logstore = 'test1'
consumer_group = 'consumer-group-5'
consumer_name1 = "consumer-group-1-A"
consumer_name2 = "consumer-group-1-B"
token = ""

if not logstore:
    logstore = 'sdk-test' + str(random.randint(1, 1000))

assert endpoint and accessKeyId and accessKey and project, ValueError("endpoint/access_id/key and "
                                                                      "project cannot be empty")

client = LogClient(endpoint, accessKeyId, accessKey, token)


def main2():
    # CRUD consumer group
    ret = client.list_consumer_group(project, logstore)
    ret.log_print()

    ret = client.list_shards(project, logstore)
    ret.log_print()

    ret = client.create_consumer_group(project, logstore, consumer_group, 30, in_order=False)
    ret.log_print()

    ret = client.heart_beat(project, logstore, consumer_group, consumer_name1, [])
    ret.log_print()

    ret = client.heart_beat(project, logstore, consumer_group, consumer_name2, [])
    ret.log_print()

    ret = client.update_check_point(project, logstore, consumer_group, 0, util.base64_encodestring('0'),
                                    consumer_name1, force_success=True)
    ret.log_print()

    ret = client.get_check_point(project, logstore, consumer_group)
    ret.log_print()

    ret = client.delete_consumer_group(project, logstore, consumer_group)
    ret.log_print()


def main1():
    option1 = LogHubConfig(endpoint, accessKeyId, accessKey, project, logstore, consumer_group,
                           consumer_name1, cursor_position=CursorPosition.BEGIN_CURSOR, heartbeat_interval=30,
                           data_fetch_interval=1)

    client_worker1 = ConsumerWorker(SampleConsumer, consumer_option=option1)
    client_worker1.start()

    time.sleep(60)

    client_worker1.shutdown()


def main3():
    option1 = LogHubConfig(endpoint, accessKeyId, accessKey, project, logstore, consumer_group,
                           consumer_name1, cursor_position=CursorPosition.BEGIN_CURSOR, heartbeat_interval=30,
                           data_fetch_interval=1)
    option2 = LogHubConfig(endpoint, accessKeyId, accessKey, project, logstore, consumer_group,
                           consumer_name2, cursor_position=CursorPosition.BEGIN_CURSOR, heartbeat_interval=30,
                           data_fetch_interval=1)

    client_worker1 = ConsumerWorker(SampleConsumer, consumer_option=option1)
    client_worker1.start()
    client_worker2 = ConsumerWorker(SampleConsumer, consumer_option=option2)
    client_worker2.start()

    time.sleep(60)

    print("*** stopping workers")
    client_worker1.shutdown()
    client_worker2.shutdown()


if __name__ == '__main__':
    main3()
