import time
from aliyun.log import *


class TestEtlCrud():
    def __init__(self, client, project, name, configuration,
                 schedule, display_name, description):
        self.client = client
        self.project = project
        self.name = name
        self.configuration = configuration
        self.schedule = schedule
        self.display_name = display_name
        self.description = description

    def test_etl_crud(self):
        # create etl job
        self.client.create_etl(self.project, self.name, self.configuration,
                               self.schedule, self.display_name, self.description)
        # get create etl job
        get_etl_config = self.client.get_etl(self.project, self.name)
        assert get_etl_config.get_etl().get("displayName") == self.display_name
        assert get_etl_config.get_etl().get("description") == self.description
        update_display_name = "test_update_etl_api"
        update_description = "test_update"
        # update etl job
        self.client.update_etl(self.project, self.name, self.configuration,
                               self.schedule, update_display_name, update_description)
        get_update_etl_config = self.client.get_etl(self.project, self.name)
        assert get_update_etl_config.get_etl().get("displayName") == update_display_name
        assert get_update_etl_config.get_etl().get("description") == update_description
        res = self.get_etl_status("RUNNING")
        assert res == True
        # stop etl job
        self.client.stop_etl(self.project, self.name)
        res = self.get_etl_status("STOPPED")
        assert res == True
        # start etl job
        self.client.start_etl(self.project, self.name)
        res = self.get_etl_status("RUNNING")
        assert res == True
        # delete etl job
        self.client.delete_etl(self.project, self.name)
        try:
            self.client.get_etl(self.project, self.name)
        except LogException as e:
            assert e.get_error_code() == "JobNotExist"
        print("test etl crud success!!!")

    def get_etl_status(self, expect_status):
        start_time = int(time.time())
        last_get_time = int(time.time())
        while True:
            if last_get_time - start_time < 300:
                current_status = self.client.get_etl(self.project, self.name).get_etl().get("status")
                print("expect_status: ", expect_status, ", current_status: ", current_status)
                if expect_status == current_status:
                    return True
                time.sleep(8)
                last_get_time = int(time.time())
            else:
                return False


if __name__ == '__main__':
    endpoint = 'cn-chengdu.log.aliyuncs.com'  # 选择与上面步骤创建Project所属区域匹配的Endpoint
    accessKeyId = ''  # 使用你的阿里云访问密钥AccessKeyId
    accessKey = ''  # 使用你的阿里云访问密钥AccessKeySecret
    project = ''  # 项目名称
    logstore = ''  # 日志库名称
    sink_logstore = ''  # 加工目标日志库名称
    name = "test-etl-api-v2"  # 加工任务名称
    display_name = "test-etl-api"  # 加工任务显示名
    configuration = {
        'accessKeyId': accessKeyId,
        'accessKeySecret': accessKey,
        'fromTime': 0,
        'logstore': logstore,
        'parameters': {},
        'roleArn': '',
        'script': 'e_set("test_time", "test_update")',
        'sinks': [
            {
                'accessKeyId': accessKeyId,
                'accessKeySecret': accessKey,
                'endpoint': '',
                'logstore': sink_logstore,
                'name': 'test',
                'project': project,
                'roleArn': '',
            }
        ],
        'toTime': 0,
        'version': 2
    }
    schedule = {
        'type': 'Resident'
    }
    # create client
    client = LogClient(endpoint, accessKeyId, accessKey)
    # test etl crud
    TestEtlCrud(client, project, name, configuration,
                schedule, display_name, "description").test_etl_crud()
