#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.


import logging
import os
import sys

from aliyun.log import LogClient

logger = logging.getLogger()
logger.setLevel(logging.INFO)
ch = logging.StreamHandler(sys.stdout)
logger.addHandler(ch)


def main():
    log_client = LogClient(endpoint=os.getenv("endpoint"), accessKeyId=os.getenv("access_key_id"),
                           accessKey=os.getenv("access_key"))
    log_client.es_migration(hosts="localhost:9200",
                            project_name=os.getenv("project_name"),
                            scroll="2m",
                            pool_size=24,
                            time_reference="es_date",
                            source="my_source",
                            topic="my_topic",
                            wait_time_in_secs=60)


if __name__ == "__main__":
    main()
