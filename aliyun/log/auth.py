#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

import locale
import urllib
from hashlib import sha256

from .util import *

AUTH_VERSION_1 = 'v1'
AUTH_VERSION_4 = 'v4'


def make_auth(access_key_id, access_key_secret, auth_version=AUTH_VERSION_1, region=''):
    if auth_version == AUTH_VERSION_4:
        return AuthV4(access_key_id, access_key_secret, region)
    else:
        return AuthV1(access_key_id, access_key_secret)


class AuthBase(object):
    def __init__(self, access_key_id, access_key_secret):
        self.access_key_id = access_key_id
        self.access_key_secret = access_key_secret

    def sign_request(self, method, resource, params, headers, body):
        pass


class AuthV1(AuthBase):

    def __init__(self, access_key_id, access_key_secret):
        super().__init__(access_key_id, access_key_secret)

    @staticmethod
    def _getGMT():
        try:
            locale.setlocale(locale.LC_TIME, "C")
        except Exception as ex:
            logger.warning("failed to set locale time to C. skip it: {0}".format(ex))
        return datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')

    def sign_request(self, method, resource, params, headers, body):
        headers['x-log-signaturemethod'] = 'hmac-sha1'
        headers['Date'] = self._getGMT()

        if body:
            headers['Content-MD5'] = Util.cal_md5(body)
        if not self.access_key_secret:
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
        signature = Util.hmac_sha1(content, self.access_key_secret)
        headers['Authorization'] = 'LOG ' + self.access_key_id + ':' + signature
        headers['x-log-date'] = headers['Date']  # bypass some proxy doesn't allow "Date" in header issue.


class AuthV4(AuthBase):
    def __init__(self, access_key_id, access_key_secret, region):
        super().__init__(access_key_id, access_key_secret)
        self._region = region

    def sign_request(self, method, resource, params, headers, body):
        content_sha256 = sha256(body).hexdigest() \
            if body else 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'
        headers['x-log-content-sha256'] = content_sha256
        current_datetime = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        headers['x-log-date'] = current_datetime
        current_date = current_datetime[:8]
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
                         + current_datetime + '\n' \
                         + scope + '\n' \
                         + sha256(canonical_request.encode('utf-8')).hexdigest()
        signature = self.build_sign_key(self.access_key_secret, self._region, current_date, string_to_sign)
        authorization = 'SLS4-HMAC-SHA256 ' + 'Credential=' \
                        + self.access_key_id + '/' + scope + ',Signature=' + signature
        headers['Authorization'] = authorization

    @staticmethod
    def build_canonical_request(method, resource, params, canonical_headers, signed_headers, hashed_payload):
        canonical_string = method + '\n' + resource + '\n'
        order_map = {}
        if params:
            for key, value in params.items():
                order_map[AuthV4.url_encode(key)] = AuthV4.url_encode(value)
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
    def url_encode(value):
        if not value:
            return ''
        encoded = urllib.parse.quote_plus(str(value))
        return encoded.replace('+', '%20').replace('*', '%2A').replace('~', '%7E').replace('/', '%2F')

    @staticmethod
    def build_sign_key(key, region, date, string_to_sign):
        sign_key = 'aliyun_v4' + key
        sign_date = hmac.new(sign_key.encode('utf-8'), date.encode('utf-8'), hashlib.sha256).digest()
        sign_region = hmac.new(sign_date, region.encode('utf-8'), hashlib.sha256).digest()
        sign_service = hmac.new(sign_region, 'sls'.encode('utf-8'), hashlib.sha256).digest()
        sign_request = hmac.new(sign_service, 'aliyun_v4_request'.encode('utf-8'), hashlib.sha256).digest()
        return hmac.new(sign_request, string_to_sign.encode('utf-8'), hashlib.sha256).hexdigest()
