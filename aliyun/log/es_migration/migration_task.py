#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.


import os
import os.path as op
import json
import traceback
from datetime import datetime
from elasticsearch.exceptions import NotFoundError
from aliyun.log import LogClient, LogException
from aliyun.log.putlogsrequest import PutLogsRequest
from aliyun.log.es_migration.doc_logitem_converter import DocLogItemConverter


class Checkpoint(object):

    processing = 'processing'
    interrupted = 'interrupted'
    finished = 'finished'
    dropped = 'dropped'
    failed = 'failed'

    def __init__(
            self, ckpt_path, task_id, es_index, es_shard,
            logstore, time_reference, logger,
    ):
        self._ckpt_file = op.join(ckpt_path, 'task_{}.json'.format(task_id))
        self._logger = logger
        self._content = {
            'status': self.processing,
            'task': {
                'es_index': es_index,
                'es_shard': es_shard,
                'logstore': logstore,
                'time_reference': time_reference,
            },
            'update_time': None,
            'progress': 0,
            'checkpoint': {
                '_id': None,
                'scroll_id': None,
            },
        }
        self._load()

    @property
    def id(self):
        return self._content['checkpoint']['_id']

    @property
    def scroll_id(self):
        return self._content['checkpoint']['scroll_id']

    @property
    def status(self):
        return self._content['status']

    @property
    def content(self):
        return self._content

    def update(self, status=processing, count=0, _id=None, scroll_id=None):
        now = datetime.now()
        self._content['update_time'] = now.isoformat(timespec='seconds')
        self._content['status'] = status
        if _id:
            self._content['checkpoint']['_id'] = _id
        self._content['progress'] += count
        if scroll_id:
            self._content['checkpoint']['scroll_id'] = scroll_id
        ckpt_bak = self._ckpt_file + '.bak'
        try:
            os.rename(self._ckpt_file, ckpt_bak)
        except FileNotFoundError:
            pass
        with open(self._ckpt_file, 'w') as fp:
            fp.write(json.dumps(self._content, indent=2))
        try:
            os.remove(ckpt_bak)
        except FileNotFoundError:
            pass

    def _load(self):
        try:
            with open(self._ckpt_file) as f:
                cont = f.read().strip()
        except FileNotFoundError:
            cont = ''
        if len(cont) > 0:
            try:
                cont = json.loads(cont)
            except json.JSONDecodeError:
                msg = 'Invalid checkpoint file content'
                extra = {'cache': cont}
                self._logger.error(msg, extra)
                raise Exception(msg)

            if cont['task'] != self._content['task']:
                msg = 'Specified task not matches with cache'
                extra = {'task': self._content['task'], 'cache': cont['task']}
                self._logger.error(msg, extra)
                raise Exception(msg)

            self._content.update(cont)


class MigrationTask(object):

    def __init__(
            self, _id, es_client, es_index, es_shard,
            logstore, ckpt_path, batch_size, logger,
            es_version, es_query=None, es_scroll="1d", time_reference=None,
    ):
        self._id = _id
        self._es_client = es_client
        self._es_index = es_index
        self._es_shard = es_shard
        self._es_query = es_query or {}
        self._es_scroll = es_scroll
        self._logstore = logstore
        self._time_reference = time_reference
        self._batch_size = batch_size
        self._logger = logger
        self._ckpt = Checkpoint(
            ckpt_path,
            self._id,
            self._es_index,
            self._es_shard,
            self._logstore.name,
            self._time_reference,
            self._logger,
        )
        self._es_params = {'preference': '_shards:{:d}'.format(self._es_shard)}
        self._es_scroll_id = self._ckpt.scroll_id
        self._status = self._ckpt.status
        self._cnt, self._last = 0, None

    def run(self, shutdown_flag):
        # already finished
        if self._ckpt.status == Checkpoint.finished:
            self._logger.info('Already finished. Ignore it.')
            return Checkpoint.finished
        self._logger.info('Migration task starts', extra=self._ckpt.content)
        try:
            self._run(shutdown_flag)
        except NotFoundError:
            self._ckpt.update(status=Checkpoint.dropped)
            self._logger.info(
                'ES index dropped',
                extra={'traceback': traceback.format_exc()},
            )
        except KeyboardInterrupt:
            self._logger.info('Migration interrupted')
            self._status = Checkpoint.interrupted
        except BaseException:
            self._logger.error(
                'Exception',
                extra={'traceback': traceback.format_exc()},
            )
            self._status = Checkpoint.failed
        finally:
            self._ckpt.update(
                status=self._status,
                count=self._cnt,
                _id=None if self._last is None else self._last.get('_id'),
                scroll_id=self._es_scroll_id,
            )

        self._logger.info('Migration task exits', extra=self._ckpt.content)
        return self._ckpt.status

    def _run(self, shutdown_flag):
        rnd = 0
        while not op.exists(shutdown_flag):
            if self._es_scroll_id is None:
                # initial search
                resp = self._es_client.search(
                    index=self._es_index,
                    body=self._es_query,
                    scroll=self._es_scroll,
                    size=self._batch_size,
                    params=self._es_params,
                )
            else:
                try:
                    resp = self._es_client.scroll(
                        scroll_id=self._es_scroll_id,
                        scroll=self._es_scroll,
                    )
                except NotFoundError:
                    msg = "cache is expired, which is with duration {}".format(self._es_scroll)
                    self._logger.error(msg, exc_info=True)
                    raise Exception(msg)

            scroll_id, hits = resp.get('_scroll_id'), resp['hits']['hits']
            if len(hits) > 0:
                self._put_docs(hits)
                self._cnt += len(hits)
                self._last = hits[-1]

            # end of scroll
            if scroll_id is None or len(hits) <= 0:
                self._status = Checkpoint.finished
                self._logger.info('Migration finished')
                break

            self._es_scroll_id = scroll_id
            rnd += 1
            if rnd % 100 == 0 or rnd == 1:
                _id = self._last.get('_id')
                self._ckpt.update(count=self._cnt, _id=_id, scroll_id=scroll_id)
                self._cnt = 0
                self._logger.info('Migration progress', extra=self._ckpt.content)

    def _put_docs(self, docs):
        if len(docs) <= 0:
            return
        logitems = [
            DocLogItemConverter.to_log_item(doc, self._time_reference)
            for doc in docs
        ]
        self._logstore.put_logs(logitems)


class MigrationLogstore(object):
    def __init__(
            self,
            endpoint,
            access_id,
            access_key,
            project_name,
            logstore_name,
            topic,
            source,
    ):
        self._log_client = LogClient(
            endpoint=endpoint,
            accessKeyId=access_id,
            accessKey=access_key,
        )
        self._project_name = project_name
        self._logstore_name = logstore_name
        self._topic, self._source = topic, source

    @property
    def name(self):
        return self._logstore_name

    def put_logs(self, logitems):
        try:
            self._log_client.put_logs(
                PutLogsRequest(
                    project=self._project_name,
                    logstore=self._logstore_name,
                    topic=self._topic,
                    source=self._source,
                    logitems=logitems,
                )
            )
        except LogException as exc:
            if exc.get_error_code() != "InvalidLogSize":
                raise
        else:  # putting succeeds
            return

        for item in logitems:
            self._log_client.put_logs(
                PutLogsRequest(
                    project=self._project_name,
                    logstore=self._logstore_name,
                    topic=self._topic,
                    source=self._source,
                    logitems=[item],
                )
            )
