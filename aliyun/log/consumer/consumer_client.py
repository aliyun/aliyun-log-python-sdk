# -×- coding: utf-8 -*-

import logging

from .exceptions import CheckPointException
from .exceptions import ClientWorkerException
from ..logexception import LogException
from ..version import USER_AGENT
from ..auth import *
from ..pulllog_response import PullLogResponse


class ConsumerClientLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        consumer_client = self.extra['consumer_client']  # type: ConsumerClient
        _id = '/'.join([
            consumer_client.mproject, consumer_client.mlogstore,
            consumer_client.mconsumer_group, consumer_client.mconsumer
        ])
        return "[{0}]{1}".format(_id, msg), kwargs


class ConsumerClient(object):
    def __init__(self, endpoint, access_key_id, access_key, project,
                 logstore, consumer_group, consumer, security_token=None, credentials_refresher=None,
                 auth_version=AUTH_VERSION_1, region='',
                 accept_compress_type=None):
        '''
        :type endpoint: string
        :param endpoint: the endpoint of sls project

        :type access_key_id: string
        :param access_key_id: your access key id

        :type access_key: string
        :param access_key: your access key secret

        :type project: string
        :param project: sls project

        :type logstore: string
        :param logstore: sls logstore to consume

        :type consumer_group: string
        :param consumer_group: consumer group name, the consumer group will be created when needed

        :type consumer: string
        :param consumer: consumer id, the id must be unique in the same consumer group

        :type security_token: string
        :param security_token: security token

        :type credentials_refresher: CredentialsProvider
        :param credentials_refresher: provides temporarily accessKeyId & accessKey dynamically

        :type auth_version: string
        :param auth_version: the auth version for signature, eg. AUTH_VERSION_1, AUTH_VERSION_4

        :type region: string
        :param region: the region of your project, required when auth_version is AUTH_VERSION_4

        :type accept_compress_type: string
        :param accept_compress_type: The compression type used for logs retrieved from sls.
        Supported types include 'lz4' and 'zstd'. If you choose 'zstd', ensure the `zstd` library is installed via pip. The default value is 'lz4'.

        '''
        from .. import LogClient

        self.mclient = LogClient(endpoint, access_key_id, access_key,
                                 security_token, auth_version=auth_version, region=region)
        self.mclient.set_user_agent(
            '%s-consumergroup-%s-%s' % (USER_AGENT, consumer_group, consumer))
        if credentials_refresher is not None:
            self.mclient.set_credentials_auto_refresher(credentials_refresher)
        self.mproject = project
        self.mlogstore = logstore
        self.mconsumer_group = consumer_group
        self.mconsumer = consumer
        self.accept_compress_type = accept_compress_type
        self.logger = ConsumerClientLoggerAdapter(
            logging.getLogger(__name__), {"consumer_client": self})

    def ensure_consumer_group_created(self, timeout, in_order):
        exists, need_fallback = self._is_consumer_group_exist()
        # fallback
        if need_fallback:
            self._ensure_consumer_group_created_legacy(timeout, in_order)
            return

        # try create
        if not exists:
            try:
                self.mclient.create_consumer_group(self.mproject, self.mlogstore, self.mconsumer_group,
                                                   timeout, in_order)
                return
            except LogException as e:
                if e.get_error_code() != 'ConsumerGroupAlreadyExist':
                    raise ClientWorkerException('error occour when create consumer group, errorCode: '
                                                + e.get_error_code() + ", errorMessage: " + e.get_error_message())

        # try update
        try:

            self.mclient.update_consumer_group(self.mproject, self.mlogstore, self.mconsumer_group,
                                               timeout, in_order)
        except LogException as e:
            raise ClientWorkerException("error occour when update consumer group, errorCode: " +
                                        e.get_error_code() + ", errorMessage: " + e.get_error_message())

    def _is_consumer_group_exist(self):
        # returns (exists, need_fallback)
        try:
            exist = (self.get_consumer_group() is not None)
            return exist, False
        except LogException as _:
            return None, True

    # fallback, for backward compatibility

    def _ensure_consumer_group_created_legacy(self, timeout, in_order):
        try:
            self.mclient.create_consumer_group(self.mproject, self.mlogstore, self.mconsumer_group,
                                               timeout, in_order)
        except LogException as e:
            # consumer group already exist
            if e.get_error_code() == 'ConsumerGroupAlreadyExist':

                try:
                    self.mclient.update_consumer_group(self.mproject, self.mlogstore, self.mconsumer_group,
                                                       timeout, in_order)

                except LogException as e1:
                    raise ClientWorkerException("error occour when update consumer group, errorCode: " +
                                                e1.get_error_code() + ", errorMessage: " + e1.get_error_message())

            else:
                raise ClientWorkerException('error occour when create consumer group, errorCode: '
                                            + e.get_error_code() + ", errorMessage: " + e.get_error_message())

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
            self.logger.warning(e)

        return False

    def update_check_point(self, shard, consumer, check_point):
        self.mclient.update_check_point(self.mproject, self.mlogstore, self.mconsumer_group,
                                        shard, check_point, consumer)

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

    def pull_logs(self, shard_id, cursor, end_cursor=None, count=1000, query=None, processor=None):
        # type: (int, str, str, int, str, object) -> PullLogResponse
        return self.mclient.pull_logs(self.mproject, self.mlogstore, shard_id, cursor, count, compress=True, end_cursor=end_cursor, query=query, accept_compress_type=self.accept_compress_type, processor=processor)
