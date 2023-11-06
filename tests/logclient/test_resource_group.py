import os

from aliyun.log import LogClient

def test_project_resource_group(client, project, resource_group_default, resource_group_new):
    client.create_project(project, '')
    res = client.get_project(project)
    assert res.get_resource_group_id() == resource_group_default

    client.change_resource_group(project, resource_group_new)
    res = client.get_project(project)
    assert res.get_resource_group_id() == resource_group_new

    res = client.list_project(resource_group_id=resource_group_new)
    assert res.get_projects()[0]['projectName'] == project

    client.delete_project(project)

if __name__ == '__main__':
    endpoint = os.environ.get('ALIYUN_LOG_SAMPLE_ENDPOINT', 'cn-hangzhou.log.aliyuncs.com')
    access_key_id = os.environ.get('ALIYUN_LOG_SAMPLE_ACCESSID', '')
    access_key = os.environ.get('ALIYUN_LOG_SAMPLE_ACCESSKEY', '')
    client = LogClient(endpoint, access_key_id, access_key)
    project = 'project-test-resource-group'
    resource_group_default = ''
    resource_group_new = ''

    test_project_resource_group(client, project, resource_group_default, resource_group_new)


