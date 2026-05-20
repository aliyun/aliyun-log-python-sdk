from aliyun.log.util import parse_timestamp


ret = parse_timestamp('2018-1-1 10:10:10 UTC')
assert ret == 1514801410

# ret = parse_timestamp('2018-1-1 10:10:10 CST')
# assert ret == 1514772610

from aliyun.log.logclient_operator import _parse_shard_list

ret = _parse_shard_list("1,2,3", list(map(str, range(1,5))))
assert  ret == list("123"), Exception(ret)
ret = _parse_shard_list("1,7-10,13", list(map(str, range(1,20))))
assert ret == "1,7,8,9,10,13".split(","), Exception(ret)
