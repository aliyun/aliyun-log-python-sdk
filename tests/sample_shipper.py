from aliyun.log import LogClient


# TODO: change me
project = "test-project"
logstore = "test-logstore"
role = "acs:ram::00000000:role/aliyunlogdefaultrole"

bucket = "test-bucket"
prefix = "test-data"

client = LogClient(
    endpoint='cn-chengdu.log.aliyuncs.com',
    accessKeyId='***',
    accessKey='***',
)

shipper = {
  "shipperName": "test-shipper",
  "targetConfiguration": {
    "bufferInterval": 300,
    "bufferSize": 32,
    "compressType": "none",
    "enable": True,
    "ossBucket": bucket,
    "ossPrefix": prefix,
    "pathFormat": "%Y/%m/%d/%H/%M",
    "roleArn": role,
    "storage": {
      "detail": {
        "enableTag": False
      },
      "format": "json"
    },
    "timeZone": ""
  },
  "targetType": "oss"
}

resp = client.create_shipper(project, logstore, shipper)
resp.log_print()
