# -*- coding: utf-8 -*-

"""
Sample code to demonstrate Object API usage.

This example shows how to use put_object and get_object methods.
"""

import os
import sys

from aliyun.log import LogClient
from aliyun.log.logexception import LogException

endpoint = "cn-hangzhou.log.aliyuncs.com"  # Replace with your endpoint
accessKeyId = ""  # Replace with your access key id
accessKey = ""  # Replace with your access key
project = "project"  # Replace with your project name
logstore = "logstore"  # Replace with your logstore name

client = LogClient(endpoint, accessKeyId, accessKey)


# Example 1: Put a simple text object
def sample_put_object():
    """
    Sample: Put an object to logstore
    """
    try:
        object_name = "test_object_1"
        content = b"Hello, this is test content"

        response = client.put_object(project, logstore, object_name, content)
        print("Put object success!")
        response.log_print()
    except LogException as e:
        print("Put object failed:", e)
        raise

    # Example 2: Put an object with custom headers
    try:
        object_name = "test_object_2"
        content = b"Content with metadata"
        headers = {
            "Content-Type": "text/plain",
            "x-log-meta-author": "test_user",
            "x-log-meta-version": "1.0",
        }

        response = client.put_object(project, logstore, object_name, content, headers)
        response.log_print()
        print("Put object with headers success!")
    except LogException as e:
        print("Put object failed:", e)
        raise

    # Example 3: Put an object with Content-MD5
    try:
        import hashlib
        import base64

        object_name = "test_object_3"
        content = b"Content with MD5"

        # Calculate MD5
        md5_hash = hashlib.md5(content).digest()
        content_md5 = base64.b64encode(md5_hash).decode("utf-8")

        headers = {
            "Content-MD5": content_md5,
            "Content-Type": "application/octet-stream",
        }

        response = client.put_object(project, logstore, object_name, content, headers)
        print("Put object with MD5 success!")
        response.log_print()
    except LogException as e:
        print("Put object failed:", e)


def sample_get_object():
    """
    Sample: Get an object from logstore
    """

    # Example: Get an object
    try:
        object_name = "test_object_1"

        response = client.get_object(project, logstore, object_name)
        response.log_print()
        print("Get object success!")
    except LogException as e:
        print("Get object failed:", e)


if __name__ == "__main__":
    print("=" * 60)
    print("Sample: Put Object")
    print("=" * 60)
    sample_put_object()

    print("\n" + "=" * 60)
    print("Sample: Get Object")
    print("=" * 60)
    sample_get_object()
