import os

from aliyun.log import LogClient


def print_tag(client, resource_type, resource_id):
    for res in client.list_tag_resources(resource_type, resource_id):
        for tag in res.get_tags():
            print(tag.get_resource_id(), "  ", tag.get_tag_key(), " : ", tag.get_tag_value())

def tag_project(client, project):
    tags = {'key1': 'value1', 'key2': 'value2'}
    client.tag_resources('project', project, **tags)
    print_tag(client, 'project', project)
    client.untag_resources('project', project, 'key1')
    print_tag(client, 'project', project)

def tag_logstore(client, project, logstore):
    tags = {'key1': 'value1', 'key2': 'value2'}
    client.tag_resources('logstore', project + '#' + logstore, **tags)
    print_tag(client, 'logstore', project + '#' + logstore)
    client.untag_resources('logstore', project + '#' + logstore, 'key1')
    print_tag(client, 'logstore', project + '#' + logstore)


def tag_dashboard(client, project, dashboard):
    tags = {'key1': 'value1', 'key2': 'value2'}
    client.tag_resources('dashboard', project + '#' + dashboard, **tags)
    print_tag(client, 'dashboard', project + '#' + dashboard)
    client.untag_resources('dashboard', project + '#' + dashboard, 'key1')
    print_tag(client, 'dashboard', project + '#' + dashboard)


if __name__ == '__main__':
    endpoint = os.environ.get('ALIYUN_LOG_SAMPLE_ENDPOINT', 'cn-hangzhou.log.aliyuncs.com')
    access_key_id = os.environ.get('ALIYUN_LOG_SAMPLE_ACCESSID', '')
    access_key = os.environ.get('ALIYUN_LOG_SAMPLE_ACCESSKEY', '')
    client = LogClient(endpoint, access_key_id, access_key)
    project = os.environ.get('ALIYUN_LOG_PROJECT', '')
    logstore = os.environ.get('ALIYUN_LOG_LOGSTORE', '')
    dashboard = os.environ.get('ALIYUN_LOG_DASHBOARD', '')

    tag_project(client, project)
    tag_logstore(client, project, logstore)
    tag_dashboard(client, project, dashboard)
