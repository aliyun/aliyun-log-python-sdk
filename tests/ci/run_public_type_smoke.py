#!/usr/bin/env python
# encoding: utf-8

import os
import subprocess
import sys
import tempfile


SMOKE_SOURCE = """\
from aliyun.log.consumer import (
    ConsumerJsonProcessorBase,
    ConsumerProcessorAdaptor,
    ConsumerProcessorBase,
    ConsumerWorker,
    CursorPosition,
    LogHubConfig,
)


class DemoProcessor(ConsumerProcessorBase):
    def process(self, log_groups, check_point_tracker):
        return None


class DemoJsonProcessor(ConsumerJsonProcessorBase):
    def process(self, flattern_json_list, check_point_tracker):
        return None


def build_processor():
    return DemoProcessor()


config = LogHubConfig(
    endpoint='cn-hangzhou.log.aliyuncs.com',
    access_key_id='ak',
    access_key='sk',
    project='project',
    logstore='logstore',
    consumer_group_name='group',
    consumer_name='consumer',
    cursor_position=CursorPosition.BEGIN_CURSOR,
)

worker = ConsumerWorker(build_processor, config)
adaptor = ConsumerProcessorAdaptor(lambda shard_id, log_groups: True)
"""


def main():
    import shutil
    temp_dir = tempfile.mkdtemp(prefix='aliyun-public-type-smoke-')
    try:
        smoke_file = os.path.join(temp_dir, 'consumer_public_smoke.py')
        with open(smoke_file, 'w') as fh:
            fh.write(SMOKE_SOURCE)

        os.chdir(temp_dir)
        cmd = [
            sys.executable,
            '-m',
            'mypy',
            '--python-version',
            '3.12',
            '--follow-imports=skip',
            '--ignore-missing-imports',
            smoke_file,
        ]
        subprocess.check_call(cmd)
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == '__main__':
    main()
