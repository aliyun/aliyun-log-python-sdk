import time
from aliyun.log import LogClient
from aliyun.log.odps_sink import AliyunMaxComputeSink
from aliyun.log.job import Export, ExportConfiguration


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


def restart_export():
    client = LogClient("region", "ak", "ak_key")
    project = '11111'
    jobName = '11111'
    config = getJobConfig(client, project, jobName)

    configuration = config['configuration']
    sinkConfig = configuration['sink']
    sink = AliyunMaxComputeSink([], [])
    sink.setOdpsRolearn(sinkConfig['odpsRolearn'])
    sink.setOdpsEndpoint(sinkConfig['odpsEndpoint'])
    sink.setOdpsTunnelEndpoint(sinkConfig['odpsTunnelEndpoint'])
    sink.setOdpsProject(sinkConfig['odpsProject'])
    sink.setOdpsTable(sinkConfig['odpsTable'])
    sink.setTimeZone(sinkConfig['timeZone'])
    sink.setFields(sinkConfig['fields'])
    sink.setPartitionColumn(sinkConfig['partitionColumn'])
    sink.setPartitionTimeFormat(sinkConfig['partitionTimeFormat'])

    exportConfiguration = ExportConfiguration()
    exportConfiguration.setRoleArn(configuration['roleArn'])
    exportConfiguration.setLogstore(configuration['logstore'])
    exportConfiguration.setSink(sink)
    exportConfiguration.setFromTime(configuration['fromTime'])
    exportConfiguration.setToTime(configuration['toTime'])

    export = Export()
    export.setConfiguration(exportConfiguration)
    export.setName(config['name'])
    export.setDisplayName(config['displayName']+'new') # 说明：本示例更新了 displayName
    client.restart_export(project_name=project, job_name=jobName, export=export)


def main():
    restart_export()


if __name__ == "__main__":
    main()
