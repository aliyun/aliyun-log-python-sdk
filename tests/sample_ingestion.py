# encoding: utf-8
import os
from aliyun.log import *


def main():
    endpoint = os.environ.get('ALIYUN_LOG_SAMPLE_ENDPOINT', '')
    access_key_id = os.environ.get('ALIYUN_LOG_SAMPLE_ACCESSID', '')
    access_key = os.environ.get('ALIYUN_LOG_SAMPLE_ACCESSKEY', '')
    project = os.environ.get('ALIYUN_LOG_SAMPLE_PROJECT', '')

    client = LogClient(endpoint, access_key_id, access_key)

    ingest_config = '{"schedule":{"delay":0,"runImmediately":true,"interval":"5m","type":"FixedRate"},"lastModifiedTime":1596087431,"recyclable":false,"configuration":{"source":{"bucket":"test-lichao","endpoint":"oss-cn-hangzhou-internal.aliyuncs.com","roleARN":"acs:ram::1049446484210612:role/aliyunlogimportossrole","prefix":" 2019/08/20/13/","compressionCodec":"snappy","restoreObjectEnabled":false,"pattern":"","format":{"skipInvalidRows":false,"timeField":"","type":"JSON"},"type":"AliyunOSS","encoding":"UTF-8"},"logstore":"233"},"createTime":1596087431,"displayName":"osstest","name":"ingest-1596087431-683090","description":"","state":"Enabled","type":"Ingestion","status":"SUCCESSED"}'
    res = client.create_ingestion(project, ingest_config)
    print res.log_print()

    res = client.list_ingestion(project)
    print res.get_ingestions()

    res = client.get_ingestion(project, "ingest-1596087431-683089")
    print res.log_print()

    res = client.stop_ingestion(project, "ingest-1596087431-683089")
    print res.log_print()

    res = client.start_ingestion(project, "ingest-1596087431-683089")
    print res.log_print()

    ingest_config = '{"schedule":{"delay":0,"runImmediately":true,"interval":"5m","type":"FixedRate"},"lastModifiedTime":1596087431,"recyclable":false,"configuration":{"source":{"bucket":"test-lichao","endpoint":"oss-cn-hangzhou-internal.aliyuncs.com","roleARN":"acs:ram::1049446484210612:role/aliyunlogimportossrole","prefix":" 2019/08/20/13/","compressionCodec":"snappy","restoreObjectEnabled":false,"pattern":"","format":{"skipInvalidRows":false,"timeField":"","type":"JSON"},"type":"AliyunOSS","encoding":"UTF-8"},"logstore":"666"},"createTime":1596087431,"displayName":"osstest","name":"ingest-1596087431-683089","description":"","state":"Enabled","type":"Ingestion","status":"SUCCESSED"}'
    res = client.update_ingestion(project, "ingest-1596087431-683089", ingest_config)
    print res.log_print()

    res = client.delete_ingestion(project, "ingest-1596087431-683089")
    print res.log_print()


if __name__ == '__main__':
    main()
