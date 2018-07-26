#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.


import logging
import os
import sys

from aliyun.log.es_migration.migration_manager import MigrationManager

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
                                         pool_size=24,
                                         time_reference="es_date",
                                         source="my_source",
                                         topic="my_topic",
                                         wait_time_in_secs=60)
    migration_manager.migrate()


if __name__ == "__main__":
    main()
