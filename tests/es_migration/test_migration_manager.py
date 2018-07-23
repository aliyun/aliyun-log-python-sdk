#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.


import os

from aliyun.log.es_migration.migration_manager import MigrationManager

import logging
import sys

logging.StreamHandler(sys.stdout)
root = logging.getLogger()
root.setLevel(logging.INFO)

ch = logging.StreamHandler(sys.stdout)
root.addHandler(ch)


def main():
    migration_manager = MigrationManager(hosts="localhost:9200",
                                         indexes=None,
                                         query=None,
                                         scroll="2m",
                                         endpoint=os.getenv("endpoint"),
                                         project_name=os.getenv("project_name"),
                                         access_key_id=os.getenv("access_key_id"),
                                         access_key=os.getenv("access_key"),
                                         logstore_index_mappings=None,
                                         pool_size=10,
                                         time_reference=None,
                                         source="my_source",
                                         topic="my_topic",
                                         wait_time_in_secs=60)
    migration_manager.migrate()


if __name__ == "__main__":
    main()
