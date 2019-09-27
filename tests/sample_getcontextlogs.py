# encoding: utf-8
from __future__ import print_function
from aliyun.log import *
import time
import os


def extract_pack_info(log):
    contents = log.get_contents()
    return contents.get('__tag__:__pack_id__', None), contents.get('__pack_meta__', None)


# @log_enter_exit
def sample_get_context_logs(client, project, logstore):
    print('please make sure your logstore has already created index')
    time.sleep(3)

    # Use get_logs and with_pack_meta to get pack information of the start log.
    # Query time range: recent 15 minutes.
    # The start log is the first log returned.
    query = '* | with_pack_meta'
    request = GetLogsRequest(project, logstore, int(time.time()) - 900, int(time.time()), '', query)
    response = client.get_logs(request)
    logs = response.get_logs()
    if not logs:
        print('no log is queried')
        return
    pack_id, pack_meta = extract_pack_info(logs[int(len(logs) / 2)])
    if pack_id is None or pack_meta is None:
        print('incomplete pack information, please make sure your logs are collected through logtail')
        print('pack_id:', pack_id)
        print('pack_meta:', pack_meta)
        return
    print('start log, pack_id:', pack_id, 'pack_meta:', pack_meta)

    # Get context logs of the start log (both directions)
    response = client.get_context_logs(project, logstore, pack_id, pack_meta, 30, 30)
    print('total lines:', response.get_total_lines())
    print('back lines:', response.get_back_lines())
    print('forward lines:', response.get_forward_lines())
    time.sleep(1)

    logs = response.get_logs()
    backward_start_log = logs[0]
    forward_start_log = logs[-1]

    # Get context logs backward.
    log = backward_start_log
    for _ in range(0, 3):
        pack_id, pack_meta = extract_pack_info(log)
        response = client.get_context_logs(project, logstore, pack_id, pack_meta, 10, 0)
        print('backward log, pack_id:', pack_id, 'pack_meta:', pack_meta)
        print('total lines:', response.get_total_lines())
        print('back lines:', response.get_back_lines())
        logs = response.get_logs()
        if not logs:
            break
        log = logs[0]
        time.sleep(1)

    # Get context logs forward.
    log = forward_start_log
    for _ in range(0, 3):
        pack_id, pack_meta = extract_pack_info(log)
        response = client.get_context_logs(project, logstore, pack_id, pack_meta, 0, 10)
        print('forward log, pack_id:', pack_id, 'pack_meta:', pack_meta)
        print('total lines:', response.get_total_lines())
        print('back lines:', response.get_back_lines())
        logs = response.get_logs()
        if not logs:
            break
        log = logs[-1]
        time.sleep(1)


def main():
    endpoint = os.environ.get('ALIYUN_LOG_SAMPLE_ENDPOINT', 'cn-hangzhou.log.aliyuncs.com')
    access_key_id = os.environ.get('ALIYUN_LOG_SAMPLE_ACCESSID', '')
    access_key = os.environ.get('ALIYUN_LOG_SAMPLE_ACCESSKEY', '')
    logstore = os.environ.get('ALIYUN_LOG_SAMPLE_LOGSTORE', '')
    project = os.environ.get('ALIYUN_LOG_SAMPLE_PROJECT', '')
    token = ""

    assert endpoint and access_key_id and access_key and project, ValueError("endpoint/access_id/key and "
                                                                             "project cannot be empty")
    client = LogClient(endpoint, access_key_id, access_key, token)
    sample_get_context_logs(client, project, logstore)


if __name__ == '__main__':
    main()
