# -*- coding: utf-8 -*-


from aliyun.log.auth import *

assert AuthV4._v4_uri_encode('123abc!@#$%^&*()-=_+ ~|\\/') \
       == '123abc%21%40%23%24%25%5E%26%2A%28%29-%3D_%2B%20~%7C%5C%2F'
assert AuthV4._v4_uri_encode(u'!@#$%^&*()=-+ ~./_[()]%20你好\0\u0111❤😓') \
       == '%21%40%23%24%25%5E%26%2A%28%29%3D-%2B%20~.%2F_%5B%28%29%5D%2520%E4%BD%A0%E5%A5%BD%00%C4%91%E2%9D%A4%F0%9F%98%93'

auth = AuthV4('acsddda21dsd', 'zxasdasdasw2', 'cn-hangzhou')
headers = {
    'hello': 'world',
    'hello-Text': 'a12X- ',
    ' Ko ': '',
    'x-log-test': 'het123',
    'x-acs-ppp': 'dds',
}
urlParams = {
    ' abc': 'efg',
    ' agc ': '',
    '': 'efg',
    'A-bc': 'eFg',
}
body = 'adasd= -asd zcas'
# method, resource, params, headers, body, current_time
assert auth._do_sign_request('POST', '/logstores', urlParams, headers, body, '20220808T032330Z') \
       == 'SLS4-HMAC-SHA256 Credential=acsddda21dsd/20220808/cn-hangzhou/sls/aliyun_v4_request,Signature=a98f5632e93836e63839cd836a54055f480020a9364ca944e2d34f2eb9bf1bed'

auth._region = 'cn-shanghai'
header2 = {}
urlParams2 = {}
assert auth._do_sign_request('POST', '/logstores', urlParams2, header2, body, '20220808T032330Z') \
       == 'SLS4-HMAC-SHA256 Credential=acsddda21dsd/20220808/cn-shanghai/sls/aliyun_v4_request,Signature=8a10a5e723cb2e75964816de660b2c16a58af8bc0261f7f0722d832468c76ce8'

auth._region = 'cn-hangzhou'
assert auth._do_sign_request('POST', '/logstores', urlParams, headers, '', '20220808T032330Z') \
       == 'SLS4-HMAC-SHA256 Credential=acsddda21dsd/20220808/cn-hangzhou/sls/aliyun_v4_request,Signature=5a66d8f8051983e0e9d08e0f960ef9252ef971eead5bb5c7acec8617a2eb2701'

assert auth._do_sign_request('GET', '/logstores', urlParams, headers, '', '20220808T032330Z') \
       == 'SLS4-HMAC-SHA256 Credential=acsddda21dsd/20220808/cn-hangzhou/sls/aliyun_v4_request,Signature=d92741852500791d662a8d469ff61627c0559ecd86c3f59b7bf6772b6c62666a'

urlParams['abs-ij*asd/vc'] = 'a~js+d ada'
urlParams['a abAas123/vc'] = 'a~jdad a2ADFs+d ada'
assert auth._do_sign_request('POST', '/logstores/hello/a+*~bb/cc', urlParams, headers, body, '20220808T032330Z') \
       == 'SLS4-HMAC-SHA256 Credential=acsddda21dsd/20220808/cn-hangzhou/sls/aliyun_v4_request,Signature=2c204068e961a8813a6bcf7ac422f7fa6e9bf9a5da493e0165dfe100854d18ff'
