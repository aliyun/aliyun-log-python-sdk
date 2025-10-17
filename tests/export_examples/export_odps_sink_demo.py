import time
from aliyun.log import LogClient
from aliyun.log.odps_sink import AliyunMaxComputeSink
from aliyun.log.job import Export, ExportConfiguration
import json


def create_export():
    project = "my-test-project"
    sink = AliyunMaxComputeSink([], [])
    sink.setOdpsRolearn("my-test-roleArn")
    sink.setOdpsEndpoint("my-test-endpoint")
    sink.setOdpsTunnelEndpoint("my-test-tunnelendpoint")
    sink.setOdpsProject("test")
    sink.setOdpsTable("my_test_table")
    sink.setTimeZone("+0800")
    sink.setFields(["acc_access_region", "http_method", "referer", "client_ip"])
    sink.setPartitionColumn(["bucket"])
    sink.setPartitionTimeFormat("%Y")
    exportConfiguration = ExportConfiguration()
    exportConfiguration.setRoleArn("my-test-roleArn")
    exportConfiguration.setLogstore("oss-source")
    exportConfiguration.setSink(sink)
    exportConfiguration.setFromTime(int(time.time()) - 864000)
    exportConfiguration.setToTime(0)
    export = Export()
    export.setConfiguration(exportConfiguration)
    export.setName("my-odps-sink")
    export.setDisplayName("my-odps-sink")
    client = LogClient("region", "ak", "ak_key")
    response = client.create_export(project, export)
    print(response.get_request_id())
    print(response.get_all_headers())


def getJobConfig(client, project, jobName):
    res = client.get_export(project, jobName)
    return res.body


def update_export():
    # 本示例演示更新displayName和delaySeconds参数的值
    client = LogClient("region", "ak", "ak_key")
    project = '11111'
    jobName = '11111'
    export = getJobConfig(client, project, jobName)  # 获取任务的配置
    export['displayName'] = export['displayName'] + 'new'

    client.update_export(project_name=project, job_name=jobName, export=export)


def main():
    update_export()


if __name__ == "__main__":
    main()
