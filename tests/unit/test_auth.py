# -*- coding: utf-8 -*-
"""Unit tests for AuthV4 signing and URI encoding."""

from aliyun.log.auth import AuthV4
from aliyun.log.credentials import StaticCredentialsProvider


def test_v4_uri_encode_ascii():
    assert AuthV4._v4_uri_encode('123abc!@#$%^&*()-=_+ ~|\\/') \
        == '123abc%21%40%23%24%25%5E%26%2A%28%29-%3D_%2B%20~%7C%5C%2F'


def test_v4_uri_encode_unicode():
    assert AuthV4._v4_uri_encode(u'!@#$%^&*()=-+ ~./_[()]%20你好\0đ❤\U0001f613') \
        == '%21%40%23%24%25%5E%26%2A%28%29%3D-%2B%20~.%2F_%5B%28%29%5D%2520%E4%BD%A0%E5%A5%BD%00%C4%91%E2%9D%A4%F0%9F%98%93'


def _make_auth(region='cn-hangzhou'):
    provider = StaticCredentialsProvider('acsddda21dsd', 'zxasdasdasw2')
    return AuthV4(provider, region)


def _common_headers():
    return {
        'hello': 'world',
        'hello-Text': 'a12X- ',
        ' Ko ': '',
        'x-log-test': 'het123',
        'x-acs-ppp': 'dds',
    }


def _common_url_params():
    return {
        ' abc': 'efg',
        ' agc ': '',
        '': 'efg',
        'A-bc': 'eFg',
    }


BODY = b'adasd= -asd zcas'


def test_sign_post_logstores():
    auth = _make_auth()
    sig = auth._do_sign_request('POST', '/logstores', _common_url_params(), _common_headers(), BODY, '20220808T032330Z')
    assert sig == ('SLS4-HMAC-SHA256 Credential=acsddda21dsd/20220808/cn-hangzhou/sls/aliyun_v4_request,'
                   'Signature=a98f5632e93836e63839cd836a54055f480020a9364ca944e2d34f2eb9bf1bed')


def test_sign_post_logstores_other_region():
    auth = _make_auth()
    auth._region = 'cn-shanghai'
    sig = auth._do_sign_request('POST', '/logstores', {}, {}, BODY, '20220808T032330Z')
    assert sig == ('SLS4-HMAC-SHA256 Credential=acsddda21dsd/20220808/cn-shanghai/sls/aliyun_v4_request,'
                   'Signature=8a10a5e723cb2e75964816de660b2c16a58af8bc0261f7f0722d832468c76ce8')


def test_sign_post_empty_body():
    auth = _make_auth()
    sig = auth._do_sign_request('POST', '/logstores', _common_url_params(), _common_headers(), '', '20220808T032330Z')
    assert sig == ('SLS4-HMAC-SHA256 Credential=acsddda21dsd/20220808/cn-hangzhou/sls/aliyun_v4_request,'
                   'Signature=5a66d8f8051983e0e9d08e0f960ef9252ef971eead5bb5c7acec8617a2eb2701')


def test_sign_get_empty_body():
    auth = _make_auth()
    sig = auth._do_sign_request('GET', '/logstores', _common_url_params(), _common_headers(), '', '20220808T032330Z')
    assert sig == ('SLS4-HMAC-SHA256 Credential=acsddda21dsd/20220808/cn-hangzhou/sls/aliyun_v4_request,'
                   'Signature=d92741852500791d662a8d469ff61627c0559ecd86c3f59b7bf6772b6c62666a')


def test_sign_post_with_extra_params():
    auth = _make_auth()
    params = _common_url_params()
    params['abs-ij*asd/vc'] = 'a~js+d ada'
    params['a abAas123/vc'] = 'a~jdad a2ADFs+d ada'
    sig = auth._do_sign_request('POST', '/logstores/hello/a+*~bb/cc', params, _common_headers(), BODY, '20220808T032330Z')
    assert sig == ('SLS4-HMAC-SHA256 Credential=acsddda21dsd/20220808/cn-hangzhou/sls/aliyun_v4_request,'
                   'Signature=2c204068e961a8813a6bcf7ac422f7fa6e9bf9a5da493e0165dfe100854d18ff')
