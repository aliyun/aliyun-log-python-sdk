from aliyun.log import LogClient


def test_project_tags():
    client = LogClient(
        endpoint='cn-chengdu.log.aliyuncs.com',
        accessKeyId='***',
        accessKey='***',
    )

    tags = {
        "normal": "of course",
        "normal2": "of course",
        "tag name": "what...?",
        "中文": "我是“测试数据”",
    }
    client.tag_project("my-project", another_tag="show it", **tags)
    client.untag_project("my-project", "normal", "normal2")

    for resp in client.get_project_tags("my-project"):
        for tag_key, tag_value in resp.get_tags().items():
            print(tag_key, "==>", tag_value)
