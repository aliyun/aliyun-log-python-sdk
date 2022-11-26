#!/usr/bin/env python
# encoding: utf-8

from .util import *
from hashlib import sha256
import urllib

AUTH_VERSION_1 = 'v1'
AUTH_VERSION_4 = 'v4'

class ProviderAuth():
    @staticmethod
    def sign(method, resource, access_id, access_key, params, headers, body, region, sign_version):
        """ :return bytes (PY2) or string (PY2) """
        if sign_version == AUTH_VERSION_4:
            ProviderAuth.v4_sign(method, resource, access_id, access_key, params, headers, body, region)
        else:
            ProviderAuth.v1_sign(method, resource, access_id, access_key, params, headers, body)

    @staticmethod
    def v1_sign(method, resource, access_id, access_key, params, headers, body):
        """ :return bytes (PY2) or string (PY2) """
        headers['x-log-signaturemethod'] = 'hmac-sha1'
        if body:
            headers['Content-MD5'] = Util.cal_md5(body)
        if not access_key:
            signature = six.b('')
            headers['Authorization'] = "LOG " + access_id + ':' + str(signature)
            return
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
        signature = Util.hmac_sha1(content, access_key)
        headers['Authorization'] = 'LOG ' + access_id + ':' + signature

    @staticmethod
    def v4_sign(method, resource, access_id, access_key, params, headers, body, region):
        """ :return bytes (PY2) or string (PY2) """
        # set x-log-content-sha256
        content_sha256 = sha256(body).hexdigest() if body else 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'
        headers['x-log-content-sha256'] = content_sha256
        # set Authorization
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
        canonical_request = ProviderAuth.build_canonical_request(method, resource, params, headers_to_string, signed_headers, content_sha256)
        scope = headers['Date'] + '/' + region + '/sls/aliyun_v4_request'

        string_to_sign = 'SLS4-HMAC-SHA256\n' \
                         + headers['x-log-date'] + '\n' \
                         + scope + '\n' \
                         + sha256(canonical_request.encode('utf-8')).hexdigest()
        signature = ProviderAuth.build_string_key(access_key, region, headers['Date'], string_to_sign)
        authorization = 'SLS4-HMAC-SHA256' + ' ' + 'Credential=' + access_id + '/' + scope + ',Signature=' + signature
        headers['Authorization'] = authorization

    @staticmethod
    def build_canonical_request(method, resource, params, canonical_headers, signed_headers, hashed_payload):
        canonical_string = method + '\n' + resource + '\n'
        order_map = {}
        if params:
            for key, value in params.items():
                order_map[ProviderAuth.url_encode(key)] = ProviderAuth.url_encode(value)
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
    def build_string_key(key, region, date, string_to_sign):
        sign_key = 'aliyun_v4' + key
        sign_date = hmac.new(sign_key.encode('utf-8'), date.encode('utf-8'), hashlib.sha256).digest()
        sign_region = hmac.new(sign_date, region.encode('utf-8'), hashlib.sha256).digest()
        sign_service = hmac.new(sign_region, 'sls'.encode('utf-8'), hashlib.sha256).digest()
        sign_request = hmac.new(sign_service, 'aliyun_v4_request'.encode('utf-8'), hashlib.sha256).digest()
        return hmac.new(sign_request, string_to_sign.encode('utf-8'), hashlib.sha256).hexdigest()


