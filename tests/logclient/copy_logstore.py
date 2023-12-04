from aliyun.log import *
import os

def main():
    client = LogClient(os.getenv('TEST_ENDPOINT'), os.getenv('TEST_ACCESS_KEY_ID'), os.getenv('TEST_ACCESS_KEY_SECRET'))
    project = 'test-project'
    client.create_logstore(project, 'test_logstore_1', ttl=120, shard_count=1, hot_ttl=7, infrequent_access_ttl=30, max_split_shard=64, auto_split=True)
    client.copy_logstore(from_project=project, from_logstore='test_logstore_1', to_logstore='test_logstore_2', to_project=project)
    
    logstore2 = client.get_logstore(project, 'test_logstore_2')
    assert logstore2.get_hot_ttl() == 7
    assert logstore2.get_infrequent_access_ttl() == 30
    assert logstore2.max_split_shard == 64
    assert logstore2.auto_split == True
    assert logstore2.get_ttl() == 120
    assert logstore2.get_shard_count() == 1
    client.delete_logstore(project, 'test_logstore_1')
    client.delete_logstore(project, 'test_logstore_2')

if __name__ == '__main__':
    main()