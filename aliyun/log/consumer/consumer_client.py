# -×- coding: utf-8 -*-

import logging

from .exceptions import CheckPointException
from ..logexception import LogException
from ..version import USER_AGENT
import time


RETRY_SLEEP = 60
conf_error = {
    "LogStoreNotExist",
    "ProjectNotExist",
    "Unauthorized",
    "SignatureNotMatch",
    "ConsumerGroupQuotaExceed",
}


class ConsumerClientLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        consumer_client = self.extra['consumer_client']  # type: ConsumerClient
        _id = '/'.join([
            consumer_client.mproject, consumer_client.mlogstore,
            consumer_client.mconsumer_group, consumer_client.mconsumer
        ])

        kwargs['extra'] = kwargs.get('extra', {})
        kwargs['extra'].update({
            "etl_context": """{
            "project": "%s", 
            "logstore": "%s", 
            "consumer_group": "%s", 
            "consumer": "%s"} """ % (consumer_client.mproject,
                                          consumer_client.mlogstore,
                                          consumer_client.mconsumer_group,
                                          consumer_client.mconsumer)
        })

        return "[{0}]{1}".format(_id, msg), kwargs


class ConsumerClient(object):
    def __init__(self, endpoint, access_key_id, access_key, project,
                 logstore, consumer_group, consumer,
                 security_token=None,
                 metric_fields_from_scheduler=None,
                 flush_check_metric_interval=None,
                 credentials_refresher=None,
                 ):

        from .. import LogClient

        self.mclient = LogClient(endpoint, access_key_id, access_key, security_token)
        self.mclient.set_user_agent('%s-consumergroup-%s' % (USER_AGENT, consumer_group))
        if credentials_refresher is not None:
            credentials_refresher.copy_to(self.mclient)
        self.mproject = project
        self.mlogstore = logstore
        self.mconsumer_group = consumer_group
        self.mconsumer = consumer
        # metric log
        self.metric_fields_from_scheduler = metric_fields_from_scheduler
        self.flush_check_metric_interval = flush_check_metric_interval
        self.logger = ConsumerClientLoggerAdapter(
            logging.getLogger(__name__), {"consumer_client": self})

    def create_consumer_group(self, timeout, in_order):
        last_exc = None
        for _ in range(10):
            try:
                self.mclient.create_consumer_group(self.mproject, self.mlogstore, self.mconsumer_group,
                                                   timeout, in_order)
                self.logger.info("complete create consumer group settings",
                                 extra={"event_id": "consumer_client:init:create_group_setting"})
                break
            except LogException as e:
                # consumer group already exist
                if e.get_error_code() == 'ConsumerGroupAlreadyExist':
                    self.logger.debug("consumer group already exist, try to update it with current settings.", extra={"event_id": "consumer_client:init:group_setting_already_exist"})

                    try:
                        self.mclient.update_consumer_group(self.mproject, self.mlogstore, self.mconsumer_group,
                                                           timeout, in_order)
                        self.logger.debug("complete update consumer group settings", extra={"event_id": "consumer_client:init:update_group_setting"})
                        break
                    except LogException as e1:
                        last_exc = e1
                        self._create_consumer_group_error(e, logging.WARNING)
                        time.sleep(RETRY_SLEEP)
                        continue
                else:
                    last_exc = e
                    self._create_consumer_group_error(e, logging.WARNING)
                    time.sleep(RETRY_SLEEP)
                    continue
        else:
            if last_exc:
                self._create_consumer_group_error(last_exc)
                raise last_exc

    def _create_consumer_group_error(self, exc, level=logging.ERROR):
        msg = "error occur when create consumer group" \
              ", errorCode: " + exc.get_error_code() + \
              ", errorMessage: " + exc.get_error_message()
        extra = {
            "event_id": "consumer_client:init:fail_create_group_setting",
            "reason": msg,
        }
        if exc.get_error_code() in conf_error:
            extra["error_code"] = "ConfigurationError"

        self.logger.log(level, msg, extra=extra)

    def get_consumer_group(self):
        for consumer_group in self.mclient.list_consumer_group(self.mproject, self.mlogstore).get_consumer_groups():
            if consumer_group.get_consumer_group_name() == self.mconsumer_group:
                return consumer_group

        return None

    def heartbeat(self, shards, responce=None):
        if responce is None:
            responce = []

        try:
            responce.extend(
                self.mclient.heart_beat(self.mproject, self.mlogstore,
                                        self.mconsumer_group, self.mconsumer, shards).get_shards())
            return True
        except LogException as e:
            if e.get_error_code() == 'NotExistConsumerWithBody':
                msg = "failed to heatbeat as unassigned body, directly reset to empty, detail: %s" % str(e)
                self.logger.info(msg, extra={"event_id": "consumer_client:heatbeat:non_exit_body_auto_rest"})
                responce.clear()
                return True
            else:
                self.logger.warning(e, extra={"event_id": "consumer_client:heatbeat:fail_heatbeat", "reason": str(e)})

        return False

    def update_check_point(self, shard, consumer, check_point):
        self.mclient.update_check_point(self.mproject, self.mlogstore, self.mconsumer_group,
                                        shard, check_point, consumer)
        try:
            res = self.mclient.get_cursor_time(self.mproject, self.mlogstore, shard, check_point)
            return res.get_cursor_time()
        except LogException as ex:
            self.logger.warning(
                ex,
                extra={
                    "event_id": "consumer_client:heatbeat:fail_get_cursor_time",
                    "reason": str(ex),
                    "shard_id": shard,
                    "checkpoint": check_point,
                },
            )

    def get_check_point(self, shard):
        check_points = self.mclient.get_check_point(self.mproject, self.mlogstore, self.mconsumer_group, shard) \
            .get_consumer_group_check_points()

        if check_points is None or len(check_points) == 0:
            raise CheckPointException('fail to get shard check point')
        else:
            return check_points[0]

    def get_cursor(self, shard_id, start_time):
        return self.mclient.get_cursor(self.mproject, self.mlogstore, shard_id, start_time).get_cursor()

    def get_begin_cursor(self, shard_id):
        return self.mclient.get_begin_cursor(self.mproject, self.mlogstore, shard_id).get_cursor()

    def get_end_cursor(self, shard_id):
        return self.mclient.get_end_cursor(self.mproject, self.mlogstore, shard_id).get_cursor()

    def pull_logs(self, shard_id, cursor, end_cursor=None, count=1000):
        return self.mclient.pull_logs(self.mproject, self.mlogstore, shard_id, cursor, count, compress=True, end_cursor=end_cursor)
