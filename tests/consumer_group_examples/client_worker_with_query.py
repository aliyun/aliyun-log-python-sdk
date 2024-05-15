# -*- coding: utf-8 -*-
import os
from aliyun.log.consumer import *
from threading import RLock
import time
# load connection info env and consumer group name from envs

endpoint = os.environ.get('ALIYUN_LOG_SAMPLE_ENDPOINT', '')
accessKeyId = os.environ.get('ALIYUN_LOG_SAMPLE_ACCESSID', '')
accessKey = os.environ.get('ALIYUN_LOG_SAMPLE_ACCESSKEY', '')
project = os.environ.get('ALIYUN_LOG_SAMPLE_PROJECT', '')
logstore = os.environ.get('ALIYUN_LOG_SAMPLE_LOGSTORE', '')
query = ''' 
* | where cast(body_bytes_sent as bigint) > 14000
'''

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


if __name__ == '__main__':
    workers = []
    for i in range(5):
        config = LogHubConfig(
            consumer_group_name="consumer_group_query_worker_test",
            consumer_name=f"consumer_{i}",
            endpoint=endpoint,
            project=project,
            logstore=logstore,
            access_key_id=accessKeyId,
            access_key=accessKey,
            query=query,
            cursor_position=CursorPosition.BEGIN_CURSOR,
            heartbeat_interval=6,
            data_fetch_interval=1
        )

        worker = ConsumerWorker(SampleConsumer, consumer_option=config)
        worker.start()
        workers.append(worker)

    time.sleep(60)  # Sleep for 1 minute

    for worker in workers:
        worker.shutdown()

    ret = str(SampleConsumer.log_results)
    print("*** get content:")
    print(ret)
