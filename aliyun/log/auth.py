#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

import locale
from hashlib import sha256

from .util import *

is_py2 = (sys.version_info[0] == 2)
is_py3 = (sys.version_info[0] == 3)

if is_py2:
    def to_bytes(data):
        """若输入为unicode， 则转为utf-8编码的bytes；其他则原样返回。"""
        if isinstance(data, unicode):
            return data.encode('utf-8')
        else:
            return data
elif is_py3:

    def to_bytes(data):
        """若输入为str（即unicode），则转为utf-8编码的bytes；其他则原样返回"""
        if isinstance(data, str):
            return data.encode(encoding='utf-8')
        else:
            return data

AUTH_VERSION_1 = 'v1'
AUTH_VERSION_4 = 'v4'


def make_auth(credentials_provider, auth_version=AUTH_VERSION_1, region=''):
    if auth_version == AUTH_VERSION_4:
        return AuthV4(credentials_provider, region)
    else:
        return AuthV1(credentials_provider)


class AuthBase(object):
    def __init__(self, credentials_provider):
        self.credentials_provider = credentials_provider

    def sign_request(self, method, resource, params, headers, body):
        pass


class AuthV1(AuthBase):

    def __init__(self, credentials_provider):
        AuthBase.__init__(self, credentials_provider)

    @staticmethod
    def _getGMT():
        try:
            locale.setlocale(locale.LC_TIME, "C")
        except Exception as ex:
            logger.warning("failed to set locale time to C. skip it: {0}".format(ex))
        return datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')

    def sign_request(self, method, resource, params, headers, body):
        credentials = self.credentials_provider.get_credentials()
        if credentials.get_security_token():
            headers['x-acs-security-token'] = credentials.get_security_token()

        headers['x-log-signaturemethod'] = 'hmac-sha1'
        headers['Date'] = self._getGMT()

        if body:
            headers['Content-MD5'] = Util.cal_md5(body)
        if not credentials.get_access_key_secret():
            return six.b('')
        content = method + '\n'
        if 'Content-MD5' in headers:
            content += headers['Content-MD5']
        content += '\n'
        if 'Content-Type' in headers:
            content += headers['Content-Type']
        content += '\n'
        content += headers['Date'] + '\n'
        content += Util.canonicalized_log_headers(headers)
        content += Util.canonicalized_resource(resource, params)
        signature = Util.hmac_sha1(content, credentials.get_access_key_secret())
        headers['Authorization'] = 'LOG ' + credentials.get_access_key_id() + ':' + signature
        headers['x-log-date'] = headers['Date']  # bypass some proxy doesn't allow "Date" in header issue.


class AuthV4(AuthBase):
    def __init__(self, credentials_provider, region):
        AuthBase.__init__(self, credentials_provider)
        self._region = region

    def sign_request(self, method, resource, params, headers, body):
        current_time = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        headers['Authorization'] = self._do_sign_request(method, resource, params, headers, body, current_time)

    def _do_sign_request(self, method, resource, params, headers, body, current_time):
        credentials = self.credentials_provider.get_credentials()

        if credentials.get_security_token():
            headers['x-acs-security-token'] = credentials.get_security_token()

        content_sha256 = sha256(body).hexdigest() \
            if body else 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'
        headers['x-log-content-sha256'] = content_sha256
        headers['x-log-date'] = current_time
        current_date = current_time[:8]
        canonical_headers = {}
        signed_headers = ''
        for original_key, value in headers.items():
            key = original_key.lower()
            if key == 'content-type' or key == 'host' or key.startswith('x-log-') or key.startswith('x-acs-'):
                canonical_headers[key] = value
        headers_to_string = ''
        for key, value in sorted(canonical_headers.items()):
            if len(signed_headers) > 0:
                signed_headers += ';'
            signed_headers += key
            headers_to_string += key + ':' + value.strip() + '\n'
        canonical_request = self.build_canonical_request(method, resource, params, headers_to_string,
                                                         signed_headers, content_sha256)
        scope = current_date + '/' + self._region + '/sls/aliyun_v4_request'

        string_to_sign = 'SLS4-HMAC-SHA256\n' \
                         + current_time + '\n' \
                         + scope + '\n' \
                         + sha256(canonical_request.encode('utf-8')).hexdigest()
        signature = self.build_sign_key(credentials.get_access_key_secret(),
                                        self._region, current_date, string_to_sign)
        return 'SLS4-HMAC-SHA256 Credential=%s/%s,Signature=%s' % (credentials.get_access_key_id(), scope, signature)

    @staticmethod
    def build_canonical_request(method, resource, params, canonical_headers, signed_headers, hashed_payload):
        canonical_string = method + '\n' + resource + '\n'
        order_map = {}
        if params:
            for key, value in params.items():
                order_map[key] = AuthV4._v4_uri_encode(value)
        separator = ''
        canonical_part = ''
        for key, value in sorted(order_map.items()):
            canonical_part += separator + key
            if value:
                canonical_part += '=' + value
            separator = '&'
        canonical_string += canonical_part + '\n'
        canonical_string += canonical_headers + '\n'
        canonical_string += signed_headers + '\n'
        canonical_string += hashed_payload
        return canonical_string

    @staticmethod
    def _v4_uri_encode(raw_text):
        raw_text = str(raw_text)

        res = ''
        for b in raw_text:
            if isinstance(b, int):
                c = chr(b)
            else:
                c = b
            if ('A' <= c <= 'Z') or ('a' <= c <= 'z') \
                    or ('0' <= c <= '9') or c in ['_', '-', '~', '.']:
                res += c
            else:
                res += "%{0:02X}".format(ord(c))
        return res

    @staticmethod
    def build_sign_key(key, region, date, string_to_sign):
        sign_key = 'aliyun_v4' + key
        sign_date = hmac.new(sign_key.encode('utf-8'), date.encode('utf-8'), hashlib.sha256).digest()
        sign_region = hmac.new(sign_date, region.encode('utf-8'), hashlib.sha256).digest()
        sign_service = hmac.new(sign_region, 'sls'.encode('utf-8'), hashlib.sha256).digest()
        sign_request = hmac.new(sign_service, 'aliyun_v4_request'.encode('utf-8'), hashlib.sha256).digest()
        return hmac.new(sign_request, string_to_sign.encode('utf-8'), hashlib.sha256).hexdigest()
