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
from aliyun.log import LogClient
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
                'offset': None,
            },
        }
        self._load()

    @property
    def id(self):
        return self._content['checkpoint']['_id']

    @property
    def offset(self):
        return self._content['checkpoint']['offset']

    @property
    def status(self):
        return self._content['status']

    @property
    def content(self):
        return self._content

    def update(self, status=processing, count=0, _id=None, offset=None):
        now = datetime.now()
        self._content['update_time'] = now.isoformat(timespec='seconds')
        self._content['status'] = status
        if _id:
            self._content['checkpoint']['_id'] = _id
        self._content['progress'] += count
        if offset:
            self._content['checkpoint']['offset'] = offset
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
        self._es_scroll_id = self._ckpt.offset

    def run(self):
        # already finished
        if self._ckpt.status == Checkpoint.finished:
            self._logger.info('Already finished. Ignore it.')
            return Checkpoint.finished
        self._logger.info('Migration task starts', extra=self._ckpt.content)
        try:
            self._run()
        except NotFoundError:
            self._ckpt.update(status=Checkpoint.dropped)
            self._logger.info(
                'ES index dropped',
                extra={'traceback': traceback.format_exc()},
            )
        except KeyboardInterrupt:
            self._logger.info('Interrupted')
            self._ckpt.update(status=Checkpoint.interrupted)
        except BaseException:
            self._logger.error(
                'Exception',
                extra={'traceback': traceback.format_exc()},
            )
            self._ckpt.update(status=Checkpoint.failed)

        self._logger.info('Migration task exits', extra=self._ckpt.content)
        return self._ckpt.status

    def _run(self):
        # check if document been processed before
        checking, offset, _id = False, None, None
        if self._time_reference and self._ckpt.offset:
            offset = self._ckpt.offset.get(self._time_reference)
            if offset:
                checking = True
        if self._ckpt.id:
            _id = self._ckpt.id
            checking = True
        if checking:
            self._logger.info('Scanning migrated documents starts')

        rnd = 0
        while True:
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
                resp = self._es_client.scroll(
                    scroll_id=self._es_scroll_id,
                    scroll=self._es_scroll,
                )

            hits = resp['hits']['hits']
            self._update_ckpt(self._put_docs(hits), len(hits))

            self._es_scroll_id = resp.get('_scroll_id')
            # end of scroll
            if self._es_scroll_id is None or len(hits) <= 0:
                self._ckpt.update(status=Checkpoint.finished)
                self._logger.info('Finished')
                break

            rnd += 1
            if rnd % 100 == 0:
                if checking:
                    self._logger.info('Scanning migrated documents')
                else:
                    self._logger.info(
                        'Migration progress',
                        extra=self._ckpt.content,
                    )

    def _pending_doc(self, doc, _id, offset):
        is_new = False
        if self._time_reference:
            is_new = doc['_source'][self._time_reference] > offset
        return is_new or doc['_id'] > _id

    def _update_ckpt(self, doc, count):
        if doc is None:
            return
        self._ckpt.update(count=count, _id=doc.get('_id'), offset=self._es_scroll_id)

    def _put_docs(self, docs):
        if len(docs) <= 0:
            return None
        logitems = [
            DocLogItemConverter.to_log_item(doc, self._time_reference)
            for doc in docs
        ]
        self._logstore.put_logs(logitems)
        return docs[-1]


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
        self._log_client.put_logs(
            PutLogsRequest(
                project=self._project_name,
                logstore=self._logstore_name,
                topic=self._topic,
                source=self._source,
                logitems=logitems,
            )
        )
