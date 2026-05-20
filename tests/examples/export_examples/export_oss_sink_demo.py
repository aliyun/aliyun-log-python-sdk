import time
from aliyun.log import LogClient
from aliyun.log.oss_sink import AliyunOSSSink
from aliyun.log.job import Export, ExportConfiguration


def main():
    project = "my-test-project"
    sink = AliyunOSSSink("my-test-roleArn", "my-test-bucket", "my-prefix", "", \
                         "%Y/%m/%d/%H/%M", "time", 256, 300, "", "csv", "none", \
                         {
                             "delimiter": ",",
                             "quote": "",
                             "lineFeed": "\n",
                             "columns": ["__topic__", "__source__"],
                         }
                         )

    exportConfiguration = ExportConfiguration()
    exportConfiguration.setRoleArn("my-test-roleArn")
    exportConfiguration.setLogstore("oss-source")
    exportConfiguration.setSink(sink)
    exportConfiguration.setFromTime(int(time.time())-864000)
    exportConfiguration.setToTime(0)
    export = Export()
    export.setConfiguration(exportConfiguration)
    export.setName("my-oss-sink")
    export.setDisplayName("my-oss-sink")
    client = LogClient("region", "ak", "ak_key")
    response = client.create_export(project, export)
    print(response.get_request_id())
    print(response.get_all_headers())

if __name__ == "__main__":
    main()
