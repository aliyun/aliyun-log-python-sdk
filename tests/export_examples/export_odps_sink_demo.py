import time
from aliyun.log import LogClient
from aliyun.log.odps_sink import AliyunMaxComputeSink
from aliyun.log.job import Export, ExportConfiguration


def main():
    project = "my-test-project"
    sink = AliyunMaxComputeSink()
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
    exportConfiguration.setFromTime(int(time.time())-864000)
    exportConfiguration.setToTime(0)
    export = Export()
    export.setConfiguration(exportConfiguration)
    export.setName("my-odps-sink")
    export.setDisplayName("my-odps-sink")
    client = LogClient("region", "ak", "ak_key")
    response = client.create_export(project, export)
    print(response.get_request_id())
    print(response.get_all_headers())

if __name__ == "__main__":
    main()
