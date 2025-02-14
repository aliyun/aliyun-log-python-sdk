#!/usr/bin/env python
# encoding: utf-8

import sys
import base64
try:
    import collections.abc as collections_abc
except ImportError:
    import collections as collections_abc
import hashlib
import hmac
import socket
import six
from datetime import datetime, tzinfo, timedelta
from dateutil import parser
from .logexception import LogException
import re
import logging
import json


logger = logging.getLogger(__name__)


def base64_encodestring(s):
    if six.PY2:
        return base64.encodestring(s)
    else:
        if isinstance(s, str):
            s = s.encode('utf8')
        return base64.encodebytes(s).decode('utf8')


def base64_decodestring(s):
    if six.PY2:
        return base64.decodestring(s)
    else:
        if isinstance(s, str):
            s = s.encode('utf8')
        return base64.decodebytes(s).decode('utf8')


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
            if key[:6].lower() in ('x-log-', 'x-acs-'):  # x-log- header
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
                urlString += u"{0}={1}&".format(
                    key, value.decode('utf8') if isinstance(value, six.binary_type) else value)
            resource += '?' + urlString[:-1]
            if six.PY3:
                return resource
            else:
                return resource.encode('utf8')

        return resource

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
        elif isinstance(data, collections_abc.Mapping):
            return dict((Util.convert_unicode_to_str(k), Util.convert_unicode_to_str(v))
                        for k, v in six.iteritems(data))
        elif isinstance(data, collections_abc.Iterable) and not isinstance(data, (six.binary_type, six.string_types)):
            return type(data)(map(Util.convert_unicode_to_str, data))

        return data


    @staticmethod
    def h_v_t(header, key):
        """
        get header value with title
        try to get key from header and consider case sensitive
        e.g. header['x-log-abc'] or header['X-Log-Abc']
        :param header:
        :param key:
        :return:
        """
        if key not in header:
            key = key.title()

            if key not in header:
                raise ValueError("Unexpected header in response, missing: " + key + " headers:\n" + str(header))

        return header[key]

    @staticmethod
    def h_v_td(header, key, default):
        """
        get header value with title with default value
        try to get key from header and consider case sensitive
        e.g. header['x-log-abc'] or header['X-Log-Abc']
        :param header:
        :param key:
        :param default:
        :return:
        """
        if key not in header:
            key = key.title()

        return header.get(key, default)

    @staticmethod
    def v_or_d(v, default=None):
        """ returns default value if v is None
            else return v
        """
        return v if v is not None else default
    

ZERO = timedelta(0)


class UTC(tzinfo):
    """UTC"""

    def utcoffset(self, dt):
        return ZERO

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return ZERO


utc = UTC()


def _get_total_seconds(delta):
    return ((delta.days * 86400 + delta.seconds) * 10**6 + delta.microseconds) / 10**6


def parse_timestamp(tm):
    if isinstance(tm, (int, float)) or \
            (isinstance(tm, (six.text_type, six.binary_type)) and tm.isdigit()):
        return int(tm)

    try:
        dt = parser.parse(tm)
    except ValueError as ex:
        try:
            # try to use dateparser to parse the format.
            from dateparser import parse
            try:
                dt = parse(tm)
                if dt is None:
                    raise ex
            except Exception as e:
                logger.error("fail to parse date: {0}, detail: {1}".format(tm, e))
                raise e

        except ImportError as ex2:
            logger.exception("import error: {}".format(ex2))
            raise ex

    if sys.version_info[:2] == (2, 6):
        if dt.tzinfo is None:
            return int(_get_total_seconds(dt - datetime(1970, 1, 1)))
        return int(_get_total_seconds(dt - datetime(1970, 1, 1, tzinfo=utc)))
    elif six.PY2:
        if dt.tzinfo is None:
            return int((dt - datetime(1970, 1, 1)).total_seconds())
        return int((dt - datetime(1970, 1, 1, tzinfo=utc)).total_seconds())
    else:
        return int(dt.timestamp())


def is_stats_query(query):
    """
    check if the query is a normal search or select query
    :param query:
    :return:
    """
    if not query:
        return False

    # remove all " enclosed strings
    nq = re.sub(r'"[^"]*"', '', query)

    # check if there's | .... select
    if re.findall(r'\|.*\bselect\b', nq, re.I|re.DOTALL):
        return True

    return False


class PrefixLoggerAdapter(logging.LoggerAdapter):
    def __init__(self, prefix, extra, *args, **kwargs):
        super(PrefixLoggerAdapter, self).__init__(*args, **kwargs)
        self._prefix = prefix
        self._extra = extra

    def process(self, msg, kwargs):
        kwargs['extra'] = kwargs.get('extra', {})
        kwargs['extra'].update(self._extra)

        return "{0}{1}".format(self._prefix, msg), kwargs


def maxcompute_sink_deserialize(obj):
    return obj.getParams()


def oss_sink_deserialize(obj):
    return {
        "type": obj.getType(),
        "roleArn": obj.getRoleArn(),
        "bucket": obj.getBucket(),
        "prefix": obj.getPrefix(),
        "suffix": obj.getSuffix(),
        "pathFormat": obj.getPathFormat(),
        "pathFormatType": obj.getPathFormatType(),
        "bufferSize": obj.getBufferSize(),
        "bufferInterval": obj.getBufferInterval(),
        "timeZone": obj.getTimeZone(),
        "contentType": obj.getContentType(),
        "compressionType": obj.getCompressionType(),
        "contentDetail": obj.getContentDetail(),
    }


def export_deserialize(obj, sink):
    return json.dumps({
        "configuration": {
            "fromTime": obj.getConfiguration().getFromTime(),
            "logstore": obj.getConfiguration().getLogstore(),
            "roleArn": obj.getConfiguration().getRoleArn(),
            "sink": sink,
            "toTime": obj.getConfiguration().getToTime(),
            "version": obj.getConfiguration().getVersion(),
        },
        "displayName": obj.getDisplayName(),
        "name": obj.getName(),
        "recyclable": obj.getRecyclable(),
        "schedule": {
            "runImmediately": obj.getSchedule().isRunImmediately(),
            "type": obj.getSchedule().getType(),
        },
        "type": obj.getType(),
    })
