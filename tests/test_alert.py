# encoding: utf-8
from __future__ import print_function

from aliyun.log import *
import os


def main():
    endpoint = os.environ.get('ALIYUN_LOG_SAMPLE_ENDPOINT', 'cn-hangzhou.log.aliyuncs.com')
    accessKeyId = os.environ.get('ALIYUN_LOG_SAMPLE_ACCESSID', '')
    accessKey = os.environ.get('ALIYUN_LOG_SAMPLE_ACCESSKEY', '')
    alert_name = 'alert-1572336608-916112'
    token = ""

    project = 'k8s-log-c783b4a12f29b44efa31f655a586bb243'

    assert endpoint and accessKeyId and accessKey and project, ValueError("endpoint/access_id/key and "
                                                                          "project cannot be empty")

    client = LogClient(endpoint, accessKeyId, accessKey, token)

    disable_alert_response = client.disable_alert(project, alert_name)
    disable_alert_response.log_print()

    enable_alert_response = client.enable_alert(project, alert_name)
    enable_alert_response.log_print()


if __name__ == '__main__':
    main()
