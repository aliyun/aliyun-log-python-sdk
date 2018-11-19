#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.


import logging
import os
import sys

from aliyun.log.es_migration import MigrationManager

logger = logging.getLogger()
logger.setLevel(logging.INFO)
ch = logging.StreamHandler(sys.stdout)
logger.addHandler(ch)


def main():
    migration_manager = MigrationManager(hosts="localhost:9200",
                                         indexes="filebeat-*,bank,shakespeare",
                                         scroll="2m",
                                         endpoint=os.getenv("endpoint"),
                                         project_name=os.getenv("project_name"),
                                         access_key_id=os.getenv("access_key_id"),
                                         access_key=os.getenv("access_key"),
                                         logstore_index_mappings='{"nginx": "filebeat-*"}',
                                         wait_time_in_secs=60,
                                         auto_creation=True)
    res = migration_manager.migrate()
    print(res)


if __name__ == "__main__":
    main()
