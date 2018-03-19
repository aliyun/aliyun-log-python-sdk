from aliyun.log.util import parse_timestamp


ret = parse_timestamp('2018-1-1 10:10:10 UTC')
assert ret == 1514801410

ret = parse_timestamp('2018-1-1 10:10:10 CST')
assert ret == 1514772610

