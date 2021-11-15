from aliyun.log import LogClient, LogException
from aliyun.log.resource_params import *
import unittest

endpoint = 'cn-heyuan.log.aliyuncs.com'
accessKeyId = ''
accessKey = ''
client = LogClient(endpoint, accessKeyId, accessKey)
resource_name = "zhanghaoyu_test"
schema = [ResourceSchemaItem("name", "string", "")]
resource_type = "userdefine"
create_record_value = {"updateName": "1111"}
acl = {"policy": {"type": "all_rw"}}
record_id = "9527"
upsert_record_id = "9528"


class TestResourceCurd(unittest.TestCase):
    def setUp(self):
        self.client = client
        self.resource_name = resource_name
        self.schema = schema
        self.record_id = record_id
        self.resource_type = resource_type
        self.upsert_record_id = upsert_record_id
        self.acl = acl
        self.record_value = create_record_value

    def test_create_resource(self):
        desc = "test"
        resource = Resource()
        resource.set_resource_name(self.resource_name)
        resource.set_description(desc)
        resource.set_schema_list(self.schema)
        resource.set_acl(self.acl)
        resource.set_resource_type(self.resource_type)
        self.client.create_resource(resource)
        res = self.get_resource(self.resource_name)
        self.assertEqual(res.get_resource().get_description(), "test")

    def test_delete_resource(self):
        self.client.delete_resource(self.resource_name)
        try:
            self.get_resource(self.resource_name)
        except LogException as e:
            self.assertEqual(e.get_error_code(), "ResourceNotExist")

    def test_get_resource(self):
        res = self.get_resource(self.resource_name)
        self.assertEqual(res.get_resource().get_resource_name(), resource_name)

    def get_resource(self, re_name):
        return self.client.get_resource(re_name)

    def test_update_resource(self):
        update_schema = [ResourceSchemaItem("updateName", "long", "")]
        resource = Resource(resource_name=self.resource_name, schema_list=update_schema, description="updateTest")
        self.client.update_resource(resource)
        res = self.get_resource(self.resource_name)
        self.assertEqual(res.get_resource().get_schema()[0].get_column(), "updateName")

    def test_list_resources(self):
        res = self.client.list_resources(resource_type=self.resource_type, resource_names=[self.resource_name])
        self.assertEqual(res.get_resources()[0].get_resource_name(), self.resource_name)

    def test_create_record(self):
        record = ResourceRecord(self.record_id, "test_tag", self.record_value)
        self.client.create_resource_record(self.resource_name, record)

        res = self.get_record(self.resource_name, self.record_id)

    def test_get_record(self):
        res = self.get_record(self.resource_name, self.record_id)
        self.assertEqual(res.get_record().get_value(), self.record_value)

    def get_record(self, re_name, rid):
        return self.client.get_resource_record(re_name, rid)

    def test_list_records(self):
        res = self.client.list_resource_records(self.resource_name, record_ids=[self.record_id])
        self.assertEqual(res.get_records()[0].get_value(), self.record_value)
        res = self.client.list_resource_records(resource_name)

    def test_delete_record(self):
        try:
            self.client.delete_resource_record(resource_name, [record_id, upsert_record_id])
            self.get_record(self.resource_name, self.record_id)
        except LogException as e:
            self.assertEqual(e.get_error_code(), "ResourceRecordNotExist")

    def test_update_record(self):
        update_record_value = {"updateName": "9999"}
        update_tag = "update_tag"
        record = ResourceRecord(self.record_id, update_tag, update_record_value)
        self.client.update_resource_record(self.resource_name, record)
        res = self.get_record(self.resource_name, self.record_id)
        self.assertEqual(json.loads(res.get_record().get("value")), update_record_value)

    def test_upsert_record(self):
        update_record_value_by_upsert = {"updateName": "8888"}
        insert_record_value = {"updateName": "1234"}
        record1 = ResourceRecord(self.upsert_record_id, "", insert_record_value)
        record2 = ResourceRecord(self.record_id, "", update_record_value_by_upsert)
        self.client.upsert_resource_record(self.resource_name, [record1, record2])
        res = self.get_record(self.resource_name, self.record_id)
        self.assertEqual(res.get_record().get_value(),
                         update_record_value_by_upsert)
        res = self.get_record(self.resource_name, self.upsert_record_id)
        self.assertEqual(res.get_record().get_value(), insert_record_value)

    def test_resource_curl(self):
        # resource
        self.test_create_resource()
        self.test_get_resource()
        self.test_list_resources()
        self.test_update_resource()

    def test_record_curl(self):
        # record
        self.test_create_record()
        self.test_get_record()
        self.test_list_records()
        self.test_update_resource()
        self.test_upsert_record()

    def test_delete(self):
        # delete
        self.test_delete_record()
        self.test_delete_resource()


if __name__ == '__main__':
    suite = unittest.TestSuite()
    suite.addTest(TestResourceCurd('test_resource_curl'))
    suite.addTest(TestResourceCurd('test_record_curl'))
    suite.addTest(TestResourceCurd('test_delete'))
    runner = unittest.TextTestRunner()
    runner.run(suite)
