#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

"""
Sample code for Logtail Pipeline Config API

This sample demonstrates how to use the Logtail Pipeline Config API:
- create_logtail_pipeline_config
- get_logtail_pipeline_config
- update_logtail_pipeline_config
- list_logtail_pipeline_config
- delete_logtail_pipeline_config
"""

from __future__ import print_function
from aliyun.log.logexception import LogException
from aliyun.log.logtail_pipeline_config_detail import LogtailPipelineConfigDetail
from aliyun.log import LogClient
import json
import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def main():
    # Initialize client
    # Please replace with your own endpoint, access_key_id and access_key
    endpoint = 'cn-hangzhou.log.aliyuncs.com'
    access_key_id = 'YOUR_ACCESS_KEY_ID'
    access_key = 'YOUR_ACCESS_KEY'
    project_name = 'YOUR_PROJECT_NAME'

    client = LogClient(endpoint, access_key_id, access_key)

    # Example 1: Create a pipeline config with real-world configuration
    print("=" * 60)
    print("Example 1: Create Logtail Pipeline Config (Real-World Example)")
    print("=" * 60)

    config_name = "test-pipeline-config"

    # Define input plugin (file input with multiline support)
    inputs = [
        {
            "Type": "input_file",
            "FilePaths": ["/var/log/application/*.log"],
            "EnableContainerDiscovery": False,
            "EnableTimestampNanosecond": True,  # nano time
            "FileEncoding": "utf8",
            "MaxDirSearchDepth": 0,
            "Multiline": {
                "Mode": "custom",
                "StartPattern": r"\[\d+-\d+-\d+ \d+:\d+:\d+\.\d+\].*",
                "UnmatchedContentTreatment": "discard"
            }
        }
    ]

    # Define processor plugins (json log parsing)
    processors = [
        {
            "KeepingSourceWhenParseFail": True,
            "KeepingSourceWhenParseSucceed": True,
            "Keys": [
                "time_local",
                "time_ms",
                "level",
                "method",
                "line",
                "thread",
                "n",
                "project",
                "logstore",
                "consumerGroup",
                "consumerId",
                "content"
            ],
            "Regex": "\\w+\\s([^,]+),(\\d+)\\s(\\w+)\\s\\[([^:]+):(\\d+)]\\s\\[(\\d+):(\\d+)]\\s-\\s\\[([^\\/]+)\\/([^\\/]+)\\/([^\\/]+)\\/([^]]+)](.*)",
            "RenamedSourceKey": "raw_content",
            "SourceKey": "content",
            "Type": "processor_parse_regex_native"
        },
        {
            "SourceFormat": "%Y-%m-%d %H:%M:%S",
            "SourceKey": "time_local",
            "Type": "processor_parse_timestamp_native"
        }
    ]

    # Define flusher plugin (SLS output with full configuration)
    flushers = [
        {
            "Type": "flusher_sls",
            "Logstore": "test-logstore",
            "Endpoint": "cn-hangzhou.log.aliyuncs.com",
            "Region": "cn-hangzhou",
            "TelemetryType": "logs"
        }
    ]

    # Create pipeline config detail
    config_detail = LogtailPipelineConfigDetail(
        config_name=config_name,
        inputs=inputs,
        flushers=flushers,
        processors=processors,
        log_sample='test 2024-02-25 14:33:40,028 INFO [worker.clean_shard_consumer:166] [1911:88099248] - [rds-project/bifrost-rds/bifrost/bifrost-172.19.6.232-25]Complete call shut down for unassigned consumer shard: 6'
    )

    try:
        response = client.create_logtail_pipeline_config(
            project_name, config_detail)
        print("Create pipeline config successfully")
        response.log_print()
    except LogException as e:
        print("Failed to create pipeline config")
        print("Error code: %s, Error message: %s" %
              (e.get_error_code(), e.get_error_message()))

    # Example 2: Get pipeline config
    print("\n" + "=" * 60)
    print("Example 2: Get Logtail Pipeline Config")
    print("=" * 60)

    try:
        response = client.get_logtail_pipeline_config(
            project_name, config_name)
        print("Get pipeline config successfully")
        response.log_print()
        config = response.get_pipeline_config()
        print("\nConfig detail:")
        print(config)
    except LogException as e:
        print("Failed to get pipeline config")
        print("Error code: %s, Error message: %s" %
              (e.get_error_code(), e.get_error_message()))

    # Example 3: Update pipeline config
    print("\n" + "=" * 60)
    print("Example 3: Update Logtail Pipeline Config")
    print("=" * 60)

    # Update processors
    updated_processors = [
        {
            "KeepingSourceWhenParseFail": True,
            "KeepingSourceWhenParseSucceed": True,
            "Keys": [
                "time_local",
                "time_ms",
                "level",
                "method",
                "line",
                "thread",
                "n",
                "project",
                "logstore",
                "consumerGroup",
                "consumerId",
                "content"
            ],
            "Regex": "\\w+\\s([^,]+),(\\d+)\\s(\\w+)\\s\\[([^:]+):(\\d+)]\\s\\[(\\d+):(\\d+)]\\s-\\s\\[([^\\/]+)\\/([^\\/]+)\\/([^\\/]+)\\/([^]]+)](.*)",
            "RenamedSourceKey": "raw_content",
            "SourceKey": "content",
            "Type": "processor_parse_regex_native"
        },
        {
            "SourceFormat": "%Y-%m-%d %H:%M:%S",
            "SourceKey": "time_local",
            "Type": "processor_parse_timestamp_native"
        }
    ]

    updated_config_detail = LogtailPipelineConfigDetail(
        config_name=config_name,
        inputs=inputs,
        flushers=flushers,
        processors=updated_processors,
        log_sample='2022-06-14 11:13:29.796 | ERROR | __main__:<module>:1 - error occurred'
    )

    try:
        response = client.update_logtail_pipeline_config(
            project_name, updated_config_detail)
        print("Update pipeline config successfully")
        response.log_print()
    except LogException as e:
        print("Failed to update pipeline config")
        print("Error code: %s, Error message: %s" %
              (e.get_error_code(), e.get_error_message()))

    # Example 4: List pipeline configs
    # Note: list API returns config names only, not full config objects
    print("\n" + "=" * 60)
    print("Example 4: List Logtail Pipeline Configs")
    print("=" * 60)

    try:
        response = client.list_logtail_pipeline_config(
            project_name, offset=0, size=100)
        print("List pipeline configs successfully")
        response.log_print()
        print("\nTotal count: %d" % response.get_total())
        print("Current count: %d" % response.get_count())
        # Returns list of names: ['config1', 'config2', ...]
        print("Config names: %s" % response.get_configs())

        # To get full config details, use get_logtail_pipeline_config for each name
        if response.get_configs():
            first_config_name = response.get_configs()[0]
            print("\nFetching full details for: %s" % first_config_name)
            detail_response = client.get_logtail_pipeline_config(
                project_name, first_config_name)
            print("Config detail: %s" % detail_response.get_pipeline_config())
    except LogException as e:
        print("Failed to list pipeline configs")
        print("Error code: %s, Error message: %s" %
              (e.get_error_code(), e.get_error_message()))

    # Example 5: List with filter
    print("\n" + "=" * 60)
    print("Example 5: List Pipeline Configs with Filter")
    print("=" * 60)

    try:
        response = client.list_logtail_pipeline_config(
            project_name,
            config_name="test",
            logstore_name="test-logstore",
            offset=0,
            size=10
        )
        print("List pipeline configs with filter successfully")
        response.log_print()
    except LogException as e:
        print("Failed to list pipeline configs")
        print("Error code: %s, Error message: %s" %
              (e.get_error_code(), e.get_error_message()))

    # Example 6: Delete pipeline config
    print("\n" + "=" * 60)
    print("Example 6: Delete Logtail Pipeline Config")
    print("=" * 60)

    try:
        response = client.delete_logtail_pipeline_config(
            project_name, config_name)
        print("Delete pipeline config successfully")
        response.log_print()
    except LogException as e:
        print("Failed to delete pipeline config")
        print("Error code: %s, Error message: %s" %
              (e.get_error_code(), e.get_error_message()))

if __name__ == '__main__':
    main()
