#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.


import time
import logging
from aliyun.log import (
    LogException, LogClient, SimpleLogHandler, IndexLineConfig, IndexConfig,
)


_log_fields = [
    'asctime',
    'filename',
    'funcName',
    'levelname',
    'lineno',
    'module',
    'message',
    'process',
]
_migration_logstore = 'internal-es-migration-log'


def setup_logging(migration, endpoint, project, access_key_id, access_key):
    # setup internal logstore
    _setup_migration_logstore(endpoint, project, access_key_id, access_key)
    time.sleep(10)

    # set logging level for libs
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('elasticsearch').setLevel(logging.WARNING)

    # add aliyunlog handler
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    fields = 'level,process_id,thread_id,module,func_name'.split(',')
    handler = SimpleLogHandler(
        end_point=endpoint,
        access_key_id=access_key_id,
        access_key=access_key,
        project=project,
        log_store=_migration_logstore,
        fields=fields,
        extract_json=True,
    )
    handler.skip_message = False
    handler.built_in_root_field = 'logging'
    handler.log_tags = [
        ('__migration__', migration),
    ]
    root.addHandler(handler)


def _setup_migration_logstore(endpoint, project, access_key_id, access_key):
    log_client = LogClient(
        endpoint=endpoint,
        accessKeyId=access_key_id,
        accessKey=access_key,
    )
    try:
        log_client.create_logstore(
            project_name=project,
            logstore_name=_migration_logstore,
        )
    except LogException as exc:
        if exc.get_error_code() != "LogStoreAlreadyExist":
            raise
    try:
        tokens = [
            ',', ' ', "'", '"', ';', '=', '(', ')', '[', ']', '{', '}', '?',
            '@', '&', '<', '>', '/', ':', '\n', '\t', '\r',
        ]
        line_config = IndexLineConfig(token_list=tokens)
        config = IndexConfig(line_config=line_config)
        log_client.create_index(project, _migration_logstore, config)
    except LogException as exc:
        if exc.get_error_code() != "IndexAlreadyExist":
            raise
