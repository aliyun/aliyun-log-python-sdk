#!/usr/bin/env python
# encoding: utf-8

import base64
import collections
import hashlib
import hmac
import re
import socket
import zlib

import six


def base64_encodestring(s):
    if six.PY2:
        return base64.encodestring(s)
    else:
        if isinstance(s, str):
            s = s.encode('utf8')
        return base64.encodebytes(s).decode('utf8')


class Util(object):
    @staticmethod
    def is_row_ip(ip):
        iparray = ip.split('.')
        if len(iparray) != 4:
            return False
        for tmp in iparray:
            if not tmp.isdigit() or int(tmp) >= 256:
                return False
        pattern = re.compile(r'^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$')
        if pattern.match(ip):
            return True
        return False

    @staticmethod
    def get_host_ip(logHost):
        """ If it is not match your local ip, you should fill the PutLogsRequest
        parameter source by yourself.
        """
        s = None
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect((logHost, 80))
            ip = s.getsockname()[0]
            return ip
        except Exception:
            return '127.0.0.1'
        finally:
            if s:
                s.close()

    @staticmethod
    def compress_data(data):
        return zlib.compress(data, 6)

    @staticmethod
    def cal_md5(content):
        return hashlib.md5(content).hexdigest().upper()

    @staticmethod
    def hmac_sha1(content, key):
        if isinstance(content, six.text_type):  # hmac.new accept 8-bit str
            content = content.encode('utf-8')
        if isinstance(key, six.text_type):  # hmac.new accept 8-bit str
            key = key.encode('utf-8')

        hashed = hmac.new(key, content, hashlib.sha1).digest()
        return base64_encodestring(hashed).rstrip()

    @staticmethod
    def canonicalized_log_headers(headers):
        content = ''
        for key in sorted(six.iterkeys(headers)):
            if key[:6] == 'x-log-' or key[:6] == 'x-acs-':  # x-log- header
                content += key + ':' + str(headers[key]) + "\n"
        return content

    @staticmethod
    def url_encode(params):
        for key, value in params.items():  # urllib.urlencode accept 8-bit str
            if isinstance(value, six.text_type):
                params[key] = value.encode('utf-8')
        return six.moves.urllib.parse.urlencode(params, True)

    @staticmethod
    def canonicalized_resource(resource, params):
        if params:
            urlString = ''
            for key, value in sorted(six.iteritems(params)):
                urlString += "{0}={1}&".format(key, value.decode('utf8') if isinstance(value, six.binary_type) else value)
            resource += '?' + urlString[:-1]

        return resource

    @staticmethod
    def get_request_authorization(method, resource, key, params, headers):
        """ :return bytes (PY2) or string (PY2) """
        if not key:
            return six.b('')
        content = method + "\n"
        if 'Content-MD5' in headers:
            content += headers['Content-MD5']
        content += '\n'
        if 'Content-Type' in headers:
            content += headers['Content-Type']
        content += "\n"
        content += headers['Date'] + "\n"
        content += Util.canonicalized_log_headers(headers)
        content += Util.canonicalized_resource(resource, params)
        return Util.hmac_sha1(content, key)

    @staticmethod
    def to_ansi(data):
        pass

    @staticmethod
    def convert_unicode_to_str(data):
        """
        Py2, always translate to utf8 from unicode
        Py3, always translate to unicode
        :param data:
        :return:
        """
        if six.PY2 and isinstance(data, six.text_type):
            return data.encode('utf8')
        elif six.PY3 and isinstance(data, six.binary_type):
            return data.decode('utf8')
        elif isinstance(data, collections.Mapping):
            return dict((Util.convert_unicode_to_str(k), Util.convert_unicode_to_str(v)) for k, v in six.iteritems(data))
        elif isinstance(data, collections.Iterable) and not isinstance(data, (six.binary_type, six.string_types)):
            return type(data)(map(Util.convert_unicode_to_str, data))

        return data

    @staticmethod
    def get_json_value(json_map, key, default_value=None):
        if key in json_map:
            return json_map[key]
        return default_value
