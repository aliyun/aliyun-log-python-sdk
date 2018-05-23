#encoding: utf8

from aliyun.log.logger_hanlder import LogFields, QueuedLogHandler
from test_logtail_config import clean_project
from aliyun.log import LogClient
import logging
import logging.config
import os
from time import time, sleep
import six

def test_log_handler(end_point, access_key_id, access_key, project, logstore):
    logger = logging.getLogger(__name__)

    aliyun_logger = QueuedLogHandler(end_point, access_key_id, access_key, project, logstore)
    aliyun_logger.setLevel(logging.INFO)
    aliyun_logger.set_fields([LogFields.level, LogFields.func_name, LogFields.file_path])
    aliyun_logger.set_topic("sdk test")
    logger.addHandler(aliyun_logger)
    logger.setLevel(logging.DEBUG)

    s = time()
    for i in range(10):
        logger.info("data %d" % i)
        print(i)

    print("...finish... %s seconds" % (time() - s))


def test_log_handler_json(end_point, access_key_id, access_key, project, logstore):
    logger = logging.getLogger('json')

    aliyun_logger = QueuedLogHandler(end_point, access_key_id, access_key, project, logstore,
                                     extract_json=True,
                                     extract_json_drop_message=True,
                                     extract_json_prefix="",
                                     buildin_fields_prefix='__',
                                     buildin_fields_suffix='__'
    )
    aliyun_logger.setLevel(logging.INFO)
    aliyun_logger.set_fields([LogFields.level, LogFields.func_name, LogFields.file_path])
    aliyun_logger.set_topic("sdk test")
    logger.addHandler(aliyun_logger)
    logger.setLevel(logging.DEBUG)

    data1 = {u'a':1, u'b': 2, u'中国':u"你好", u'你好': "中国"}
    data2 = {u'a':1, u'b': 2, u'ww':u"你好", u'bb': "中国"}
    data3 = {u'ww': u"你好", u'bb': "中国"}
    data4 = "hello1"
    data5 = {'hello2': "good", "line_no": 100}
    data6 = {"hello3": 1000, "hell3_2": (1,2,3)}
    data7 = {'hello4': {"a": 1, "b": 2}, "hello4_2": ["abc", "ddd"]}

    s = time()

    data = [data1, data2, data3, data4, data5, data6, data7]
    for d in data:
        logger.info(d)

    if six.PY3:
        d = {b'a': 1, b'b': 2, '中国'.encode('utf8'): "你好".encode('utf8'), u'你好': "中国"}
        logger.info(d)

    print("...finish... %s seconds" % (time() - s))


def main():
    endpoint = os.environ.get('ALIYUN_LOG_SAMPLE_ENDPOINT', '')
    accessKeyId = os.environ.get('ALIYUN_LOG_SAMPLE_ACCESSID', '')
    accessKey = os.environ.get('ALIYUN_LOG_SAMPLE_ACCESSKEY', '')

    project = 'python-sdk-test' + str(time()).replace('.', '-')
    logstore = 'logstore'

    assert endpoint and accessKeyId and accessKey, ValueError("endpoint/access_id/key cannot be empty")

    client = LogClient(endpoint, accessKeyId, accessKey, "")

    print("****create project", project)
    client.create_project(project, "SDK test")
    sleep(10)

    try:
        print("****create logstore", logstore)
        client.create_logstore(project, logstore, 1, 1)
        sleep(60)

        test_log_handler(endpoint, accessKeyId, accessKey, project, logstore)

        sleep(60)

        res = client.pull_log(project, logstore, 0, time() - 3600, time())
        for x in res:
            print(x.get_flatten_logs_json())
            assert len(x.get_flatten_logs_json()) == 10
            break

        # test extract json
        test_log_handler_json(endpoint, accessKeyId, accessKey, project, logstore)

        sleep(60)

        # test using file to configure logger
        os.environ['ALIYUN_LOG_SAMPLE_TMP_PROJECT'] = project
        config_path = os.sep.join([os.path.dirname(__file__), 'logging.conf'])
        logging.config.fileConfig(config_path)

        # create logger
        logger = logging.getLogger('sls')
        logger.info("log hanlder test via config file")

        sleep(20)

    finally:
        clean_project(client, project)

if __name__ == '__main__':
    main()
