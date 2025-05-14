from aliyun.log import LogClient

client = LogClient('cn-hangzhou.log.aliyuncs.com',
                   'your access key id',
                   'your access key secret')

project = 'your-project'

client.create_project(project, 'hello', data_redundancy_type='LRS') # LRS or ZRS
resp = client.get_project(project)

print(resp.get_data_redundancy_type())
assert resp.get_data_redundancy_type() == 'LRS'
client.delete_project(project)
