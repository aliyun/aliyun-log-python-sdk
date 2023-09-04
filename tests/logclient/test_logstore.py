from aliyun.log import *
import unittest
import time

class TestLogStore(unittest.TestCase):
    def setUp(self):
        self._endpoint = os.environ['TEST_ENDPOINT']
        self._accessKeyId = os.environ['TEST_ACCESS_KEY_ID']
        self._accessKey = os.environ['TEST_ACCESS_KEY_SECRET']
        self._project = "ali-sls-python-sdk-test-logstore"
        self._logstore = 'test-logstore'
        self.client = LogClient(self._endpoint, self._accessKeyId, self._accessKey)
        self.client.create_project(self._project, "")
        self.client.create_logstore(self._project, logstore_name=self._logstore, shard_count=1)
        
    def tearDown(self) -> None:
        self.client.delete_project(self._project)
  
    def test_logstores_metering_mode(self):
        resp = self.client.get_logstore_metering_mode(self._project, self._logstore)
        mode = resp.get_metering_mode()
        self.assertEqual(mode, MeteringMode.ChargeByFunction)
        self.client.update_logstore_metering_mode(self._project, self._logstore, MeteringMode.ChargeByDataIngest)
        time.sleep(1)
        resp = self.client.get_logstore_metering_mode(self._project, self._logstore)
        mode = resp.get_metering_mode()
        self.assertEqual(mode, MeteringMode.ChargeByDataIngest)

if __name__ == '__main__':
    unittest.main()

