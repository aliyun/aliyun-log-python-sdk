"""
LogClient class is the main class in the SDK. It can be used to communicate with
log service server to put/get data.

:Author: Aliyun
"""

import json
import requests
import six
import time
import zlib
from datetime import datetime
import logging
from .credentials import StaticCredentialsProvider
from .scheduled_sql import ScheduledSQLConfiguration
from .scheduled_sql_response import *
from itertools import cycle
from .consumer_group_request import *
from .consumer_group_response import *
from .getlogsrequest import *
from .cursor_response import GetCursorResponse
from .cursor_time_response import GetCursorTimeResponse
from .gethistogramsresponse import GetHistogramsResponse
from .getlogsresponse import GetLogsResponse
from .getcontextlogsresponse import GetContextLogsResponse
from .index_config_response import *
from .ingestion_response import *
from .sql_instance_response import *
from .listlogstoresresponse import ListLogstoresResponse
from .listtopicsresponse import ListTopicsResponse
from .logclient_operator import copy_project, list_more, query_more, pull_log_dump, copy_logstore, copy_data, \
    get_resource_usage, arrange_shard, transform_data
from .logexception import LogException
from .logstore_config_response import *
from .substore_config_response import *
from .logtail_config_response import *
from .machinegroup_response import *
from .project_response import *
from .pulllog_response import PullLogResponse
from .putlogsresponse import PutLogsResponse
from .shard_response import *
from .shipper_response import *
from .resource_response import *
from .resource_params import *
from .tag_response import GetResourceTagsResponse
from .topostore_response import *
from .topostore_params import *
from .util import base64_encodestring as b64e
from .util import base64_encodestring as e64, base64_decodestring as d64, Util
from .version import API_VERSION, USER_AGENT

from .log_logs_raw_pb2 import LogGroupRaw as LogGroup
from .external_store_config_response import *
import struct
from .logresponse import LogResponse
from copy import copy
from .pluralize import pluralize
from .etl_config_response import *
from .export_response import *
from .common_response import *
from .auth import *

logger = logging.getLogger(__name__)

if six.PY3:
    xrange = range

lz4_available = Util.is_lz4_available()
if lz4_available:
    from .util import lz_decompress, lz_compresss

CONNECTION_TIME_OUT = 120
MAX_LIST_PAGING_SIZE = 500
MAX_GET_LOG_PAGING_SIZE = 100

DEFAULT_QUERY_RETRY_COUNT = 5
DEFAULT_QUERY_RETRY_INTERVAL = 0.5

DEFAULT_REFRESH_RETRY_COUNT = 5
DEFAULT_REFRESH_RETRY_DELAY = 30
MIN_REFRESH_INTERVAL = 300

_auth_code_set = {"Unauthorized", "InvalidAccessKeyId.NotFound", 'SecurityToken.Expired', "InvalidAccessKeyId", 'SecurityTokenExpired'}
_auth_partial_code_set = {"Unauthorized", "InvalidAccessKeyId", "SecurityToken"}


def _is_auth_err(status, code, msg):
    if code in _auth_code_set:
        return True
    for m in _auth_partial_code_set:
        if m in code or m in msg:
            return True
    return False


def _apply_cn_keys_patch():
    """
    apply this patch due to an issue in http.client.parse_headers
    when there're multi-bytes in headers. it will truncate some headers.
    https://github.com/aliyun/aliyun-log-python-sdk/issues/79
    """
    import sys
    if sys.version_info[:2] == (3, 5):
        import http.client as hc
        old_parse = hc.parse_headers

        def parse_header(*args, **kwargs):
            fp = args[0]
            old_readline = fp.readline

            def new_readline(*args, **kwargs):
                ret = old_readline(*args, **kwargs)
                if ret.lower().startswith(b'x-log-query-info'):
                    return b'x-log-query-info: \r\n'
                return ret

            fp.readline = new_readline

            ret = old_parse(*args, **kwargs)
            return ret

        hc.parse_headers = parse_header


_apply_cn_keys_patch()


class LogClient(object):
    """ Construct the LogClient with endpoint, accessKeyId, accessKey.

    :type endpoint: string
    :param endpoint: log service host name, for example, ch-hangzhou.log.aliyuncs.com or https://cn-beijing.log.aliyuncs.com

    :type accessKeyId: string
    :param accessKeyId: aliyun accessKeyId

    :type accessKey: string
    :param accessKey: aliyun accessKey
    """

    __version__ = API_VERSION
    Version = __version__

    def __init__(self, endpoint, accessKeyId=None, accessKey=None, securityToken=None, source=None,
                 auth_version=AUTH_VERSION_1, region='', credentials_provider=None):
        self._isRowIp = Util.is_row_ip(endpoint)
        self._setendpoint(endpoint)
        self._accessKeyId = accessKeyId
        self._accessKey = accessKey
        self._timeout = CONNECTION_TIME_OUT
        if source is None:
            self._source = Util.get_host_ip(self._logHost)
        else:
            self._source = source
        self._securityToken = securityToken

        self._user_agent = USER_AGENT
        self._credentials_auto_refresher = None
        self._last_refresh = 0
        self._auth_version = auth_version
        self._region = region
        if credentials_provider is not None:
            self._auth = make_auth(credentials_provider, auth_version, region)
        else:
            self._auth = make_auth(StaticCredentialsProvider(accessKeyId, accessKey, securityToken),
                                   auth_version, region)
        self._get_logs_v2_enabled = True

    def _replace_credentials(self):
        delta = time.time() - self._last_refresh
        if delta < MIN_REFRESH_INTERVAL:
            logger.warning("refresh credentials wait, because of too frequent refresh")
            time.sleep(MIN_REFRESH_INTERVAL - delta)

        logger.info("refresh credentials, start")
        self._last_refresh = time.time()
        for tries in range(DEFAULT_REFRESH_RETRY_COUNT + 1):
            try:
                self._auth = make_auth(StaticCredentialsProvider(self._credentials_auto_refresher()),
                                       self._auth_version, self._region)
            except Exception as ex:
                logger.error(
                    "failed to call _credentials_auto_refresher to refresh credentials, details: {0}".format(str(ex)))
                time.sleep(DEFAULT_REFRESH_RETRY_DELAY)
            else:
                logger.info("call _credentials_auto_refresher to auto refresh credentials successfully.")
                return

    def set_credentials_auto_refresher(self, refresher):
        self._credentials_auto_refresher = refresher

    @property
    def timeout(self):
        return self._timeout

    @timeout.setter
    def timeout(self, value):
        self._timeout = value

    def set_user_agent(self, user_agent):
        """
        set user agent

        :type user_agent: string
        :param user_agent: user agent

        :return: None

        """
        self._user_agent = user_agent

    def _setendpoint(self, endpoint):
        self.http_type = 'http://'
        self._port = 80

        endpoint = endpoint.strip()
        pos = endpoint.find('://')
        if pos != -1:
            self.http_type = endpoint[:pos + 3]
            endpoint = endpoint[pos + 3:]

        if self.http_type.lower() == 'https://':
            self._port = 443

        pos = endpoint.find('/')
        if pos != -1:
            endpoint = endpoint[:pos]
        pos = endpoint.find(':')
        if pos != -1:
            self._port = int(endpoint[pos + 1:])
            endpoint = endpoint[:pos]
        self._logHost = endpoint
        self._endpoint = endpoint + ':' + str(self._port)

    @staticmethod
    def _loadJson(resp_status, resp_header, resp_body, requestId):
        if not resp_body:
            return None
        try:
            if isinstance(resp_body, six.binary_type):
                return json.loads(resp_body.decode('utf8', "ignore"))

            return json.loads(resp_body)
        except Exception as ex:
            raise LogException('BadResponse',
                               'Bad json format:\n"%s"' % resp_body + '\n' + repr(ex),
                               requestId, resp_status, resp_header, resp_body)

    def _getHttpResponse(self, method, url, params, body, headers):  # ensure method, url, body is str
        try:
            headers['User-Agent'] = self._user_agent
            r = getattr(requests, method.lower())(url, params=params, data=body, headers=headers, timeout=self._timeout)
            return r.status_code, r.content, r.headers
        except Exception as ex:
            raise LogException('LogRequestError', str(ex))

    def _sendRequest(self, method, url, params, body, headers, respons_body_type='json'):
        (resp_status, resp_body, resp_header) = self._getHttpResponse(method, url, params, body, headers)
        header = {}
        for key, value in resp_header.items():
            header[key] = value

        requestId = Util.h_v_td(header, 'x-log-requestid', '')

        if resp_status == 200:
            if respons_body_type == 'json':
                exJson = self._loadJson(resp_status, resp_header, resp_body, requestId)
                exJson = Util.convert_unicode_to_str(exJson)
                return exJson, header
            else:
                return resp_body, header

        exJson = self._loadJson(resp_status, resp_header, resp_body, requestId)
        exJson = Util.convert_unicode_to_str(exJson)

        if 'errorCode' in exJson and 'errorMessage' in exJson:
            raise LogException(exJson['errorCode'], exJson['errorMessage'], requestId,
                               resp_status, resp_header, resp_body)
        else:
            exJson = '. Return json is ' + str(exJson) if exJson else '.'
            raise LogException('LogRequestError',
                               'Request is failed. Http code is ' + str(resp_status) + exJson, requestId,
                               resp_status, resp_header, resp_body)

    def _send(self, method, project, body, resource, params, headers, respons_body_type='json'):
        if body:
            headers['Content-Length'] = str(len(body))
        else:
            headers['Content-Length'] = '0'
            headers["x-log-bodyrawsize"] = '0'

        headers['x-log-apiversion'] = API_VERSION
        if self._isRowIp or not project:
            url = self.http_type + self._endpoint
        else:
            url = self.http_type + project + "." + self._endpoint

        if project:
            headers['Host'] = project + "." + self._logHost
        else:
            headers['Host'] = self._logHost

        retry_times = range(10) if 'log-cli-v-' not in self._user_agent else cycle(range(10))
        last_err = None
        url = url + resource
        for _ in retry_times:
            try:
                headers2 = copy(headers)
                params2 = copy(params)
                if self._securityToken:
                    headers2["x-acs-security-token"] = self._securityToken
                self._auth.sign_request(method, resource, params2, headers2, body)
                return self._sendRequest(method, url, params2, body, headers2, respons_body_type)
            except LogException as ex:
                last_err = ex
                if ex.get_error_code() in ('InternalServerError', 'RequestTimeout') or ex.resp_status >= 500\
                        or (ex.get_error_code() == 'LogRequestError'
                            and 'httpconnectionpool' in ex.get_error_message().lower()):
                    time.sleep(1)
                    continue
                elif self._credentials_auto_refresher and _is_auth_err(ex.resp_status, ex.get_error_code(), ex.get_error_message()):
                    if ex.get_error_code() not in ("SecurityToken.Expired", "SecurityTokenExpired"):
                        logger.warning(
                            "request with authentication error",
                            exc_info=True,
                            extra={"error_code": "AuthenticationError"},
                        )
                    self._replace_credentials()
                    continue
                raise

        raise last_err

    @staticmethod
    def _get_unicode(key):
        if isinstance(key, six.binary_type):
            try:
                key = key.decode('utf-8')
            except UnicodeDecodeError:
                return key
        return key

    @staticmethod
    def _get_binary(key):
        if isinstance(key, six.text_type):
            return key.encode('utf-8')
        return key

    def set_source(self, source):
        """
        Set the source of the log client

        :type source: string
        :param source: new source

        :return: None
        """
        self._source = source

    def put_log_raw(self, project, logstore, log_group, compress=None):
        """ Put logs to log service. using raw data in protobuf

        :type project: string
        :param project: the Project name

        :type logstore: string
        :param logstore: the logstore name

        :type log_group: LogGroup
        :param log_group: log group structure

        :type compress: boolean
        :param compress: compress or not, by default is True

        :return: PutLogsResponse

        :raise: LogException
        """
        body = log_group.SerializeToString()
        raw_body_size = len(body)
        headers = {'x-log-bodyrawsize': str(raw_body_size), 'Content-Type': 'application/x-protobuf'}

        if compress is None or compress:
            if lz4_available:
                headers['x-log-compresstype'] = 'lz4'
                body = lz_compresss(body)
            else:
                headers['x-log-compresstype'] = 'deflate'
                body = zlib.compress(body)

        params = {}
        resource = '/logstores/' + logstore + "/shards/lb"

        (resp, header) = self._send('POST', project, body, resource, params, headers)

        return PutLogsResponse(header, resp)

    def put_logs(self, request):
        """ Put logs to log service. up to 512000 logs up to 10MB size
        Unsuccessful operation will cause an LogException.

        :type request: PutLogsRequest
        :param request: the PutLogs request parameters class

        :return: PutLogsResponse

        :raise: LogException
        """
        if len(request.get_log_items()) > 512000:
            raise LogException('InvalidLogSize',
                               "logItems' length exceeds maximum limitation: 512000 lines. now: {0}".format(
                                   len(request.get_log_items())))
        logGroup = LogGroup()
        logGroup.Topic = request.get_topic()
        if request.get_source():
            logGroup.Source = request.get_source()
        else:
            if self._source == '127.0.0.1':
                self._source = Util.get_host_ip(request.get_project() + '.' + self._logHost)
            logGroup.Source = self._source
        for logItem in request.get_log_items():
            log = logGroup.Logs.add()
            log.Time = logItem.get_time()
            log.Time_ns = logItem.get_time_nano_part()
            contents = logItem.get_contents()
            for key, value in contents:
                content = log.Contents.add()
                content.Key = self._get_unicode(key)
                content.Value = self._get_binary(value)
        if request.get_log_tags() is not None:
            tags = request.get_log_tags()
            for key, value in tags:
                pb_tag = logGroup.LogTags.add()
                pb_tag.Key = key
                pb_tag.Value = value
        body = logGroup.SerializeToString()

        if len(body) > 10 * 1024 * 1024:  # 10 MB
            raise LogException('InvalidLogSize',
                               "logItems' size exceeds maximum limitation: 10 MB. now: {0} MB.".format(
                                   len(body) / 1024.0 / 1024))

        headers = {'x-log-bodyrawsize': str(len(body)), 'Content-Type': 'application/x-protobuf'}
        is_compress = request.get_compress()

        compress_data = None
        if is_compress:
            if lz4_available:
                headers['x-log-compresstype'] = 'lz4'
                compress_data = lz_compresss(body)
            else:
                headers['x-log-compresstype'] = 'deflate'
                compress_data = zlib.compress(body)

        params = {}
        logstore = request.get_logstore()
        project = request.get_project()
        if request.get_hash_key() is not None:
            resource = '/logstores/' + logstore + "/shards/route"
            params["key"] = request.get_hash_key()
        else:
            resource = '/logstores/' + logstore + "/shards/lb"

        if is_compress:
            (resp, header) = self._send('POST', project, compress_data, resource, params, headers)
        else:
            (resp, header) = self._send('POST', project, body, resource, params, headers)

        return PutLogsResponse(header, resp)

    def list_logstores(self, request):
        """ List all logstores of requested project.
        Unsuccessful operation will cause an LogException.

        :type request: ListLogstoresRequest
        :param request: the ListLogstores request parameters class.

        :return: ListLogStoresResponse

        :raise: LogException
        """
        headers = {}
        params = {}
        resource = '/logstores'
        project = request.get_project()
        (resp, header) = self._send("GET", project, None, resource, params, headers)
        return ListLogstoresResponse(resp, header)

    def list_topics(self, request):
        """ List all topics in a logstore.
        Unsuccessful operation will cause an LogException.

        :type request: ListTopicsRequest
        :param request: the ListTopics request parameters class.

        :return: ListTopicsResponse

        :raise: LogException
        """
        headers = {}
        params = {}
        if request.get_token() is not None:
            params['token'] = request.get_token()
        if request.get_line() is not None:
            params['line'] = request.get_line()
        params['type'] = 'topic'
        logstore = request.get_logstore()
        project = request.get_project()
        resource = "/logstores/" + logstore
        (resp, header) = self._send("GET", project, None, resource, params, headers)

        return ListTopicsResponse(resp, header)

    def get_histograms(self, request):
        """ Get histograms of requested query from log service.
        Unsuccessful operation will cause an LogException.

        :type request: GetHistogramsRequest
        :param request: the GetHistograms request parameters class.

        :return: GetHistogramsResponse

        :raise: LogException
        """
        headers = {}
        params = {}
        if request.get_topic() is not None:
            params['topic'] = request.get_topic()
        if request.get_from() is not None:
            params['from'] = request.get_from()
        if request.get_to() is not None:
            params['to'] = request.get_to()
        if request.get_query() is not None:
            params['query'] = request.get_query()

        params['accurate'] = request.get_accurate_query()
        params['fromNs'] = request.get_from_time_nano_part()
        params['toNs'] = request.get_to_time_nano_part()
        if request.get_shard_id() != -1:
            params['shard'] = request.get_shard_id()
        params['type'] = 'histogram'
        logstore = request.get_logstore()
        project = request.get_project()
        resource = "/logstores/" + logstore
        (resp, header) = self._send("GET", project, None, resource, params, headers)
        return GetHistogramsResponse(resp, header)

    def get_log(self, project, logstore, from_time, to_time, topic=None,
                query=None, reverse=False, offset=0, size=100, power_sql=False, scan=False, forward=True, accurate_query=True, from_time_nano_part=0, to_time_nano_part=0):
        """ Get logs from log service.
        will retry DEFAULT_QUERY_RETRY_COUNT when incomplete.
        Unsuccessful operation will cause an LogException.
        Note: for larger volume of data (e.g. > 1 million logs), use get_log_all

        :type project: string
        :param project: project name

        :type logstore: string
        :param logstore: logstore name

        :type from_time: int/string
        :param from_time: the begin timestamp or format of time in readable time like "%Y-%m-%d %H:%M:%S<time_zone>" e.g. "2018-01-02 12:12:10+8:00", also support human readable string, e.g. "1 hour ago", "now", "yesterday 0:0:0", refer to https://aliyun-log-cli.readthedocs.io/en/latest/tutorials/tutorial_human_readable_datetime.html

        :type to_time: int/string
        :param to_time: the end timestamp or format of time in readable time like "%Y-%m-%d %H:%M:%S<time_zone>" e.g. "2018-01-02 12:12:10+8:00", also support human readable string, e.g. "1 hour ago", "now", "yesterday 0:0:0", refer to https://aliyun-log-cli.readthedocs.io/en/latest/tutorials/tutorial_human_readable_datetime.html

        :type topic: string
        :param topic: topic name of logs, could be None

        :type query: string
        :param query: user defined query, could be None

        :type reverse: bool
        :param reverse: if reverse is set to true, the query will return the latest logs first, default is false

        :type offset: int
        :param offset: line offset of return logs

        :type size: int
        :param size: max line number of return logs, -1 means get all

        :type power_sql: bool
        :param power_sql: if power_sql is set to true, the query will run on enhanced sql mode

        :type scan: bool
        :param scan: if scan is set to true, the query will use scan mode

        :type forward: bool
        :param forward: only for scan query, if forward is set to true, the query will get next page, otherwise previous page

        :type accurate_query: bool
        :param accurate_query: if accurate_query is set to true, the query will global ordered time second mode

        :type from_time_nano_part: int
        :param from_time_nano_part: nano part of query begin time

        :type to_time_nano_part: int
        :param to_time_nano_part: nano part of query end time

        :return: GetLogsResponse

        :raise: LogException
        """
        # need to use extended method to get more when:
        # it's not select query, and size > default page size
        size, offset = int(size), int(offset)
        if not is_stats_query(query) and (size == -1 or size > MAX_GET_LOG_PAGING_SIZE):
            return query_more(
                self.get_log,
                offset=offset,
                size=size,
                batch_size=MAX_GET_LOG_PAGING_SIZE,
                project=project,
                logstore=logstore,
                from_time=from_time,
                to_time=to_time,
                topic=topic,
                query=query,
                reverse=reverse,
                accurate_query=accurate_query,
                from_time_nano_part=from_time_nano_part,
                to_time_nano_part=to_time_nano_part
            )

        ret = None
        for _c in xrange(DEFAULT_QUERY_RETRY_COUNT):
            headers = {}
            params = {'from': parse_timestamp(from_time),
                      'to': parse_timestamp(to_time),
                      'line': size,
                      'offset': offset,
                      'powerSql': power_sql,
                      'accurate': accurate_query,
                      'fromNs': from_time_nano_part,
                      'toNs': to_time_nano_part
                      }

            if topic:
                params['topic'] = topic
            if query:
                params['query'] = query
            if scan:
                params['session'] = 'mode=scan'
                params['forward'] = 'true' if forward else 'false'
            
            if self._get_logs_v2_enabled:
                resource = "/logstores/" + logstore + "/logs"
                headers["Content-Type"] = "application/json"
                params['reverse'] = reverse
                params['forward'] = forward
                body_str = six.b(json.dumps(params))
                headers["x-log-bodyrawsize"] = str(len(body_str))
                accept_encoding = "lz4" if lz4_available else "deflate"
                headers['Accept-Encoding'] = accept_encoding

                (resp, header) = self._send("POST", project, body_str, resource, None, headers, respons_body_type=accept_encoding)

                raw_data = Util.uncompress_response(header, resp)
                exJson = self._loadJson(200, header, raw_data, requestId=Util.h_v_td(header, 'x-log-requestid', ''))
                exJson = Util.convert_unicode_to_str(exJson)
                ret = GetLogsResponse(exJson, header)
            else:
                resource = "/logstores/" + logstore
                params['type'] = 'log'
                params['reverse'] = 'true' if reverse else 'false'
                (resp, header) = self._send("GET", project, None, resource, params, headers)
                ret = GetLogsResponse._from_v1_resp(resp, header)
            if ret.is_completed():
                break

            time.sleep(DEFAULT_QUERY_RETRY_INTERVAL)

        return ret

    def get_logs(self, request):
        """ Get logs from log service.
        will retry DEFAULT_QUERY_RETRY_COUNT when incomplete.
        Unsuccessful operation will cause an LogException.
        Note: for larger volume of data (e.g. > 1 million logs), use get_log_all

        :type request: GetLogsRequest
        :param request: the GetLogs request parameters class.

        :return: GetLogsResponse

        :raise: LogException
        """
        project = request.get_project()
        logstore = request.get_logstore()
        from_time = request.get_from()
        to_time = request.get_to()
        topic = request.get_topic()
        query = request.get_query()
        reverse = request.get_reverse()
        offset = request.get_offset()
        size = request.get_line()
        power_sql = request.get_power_sql()
        scan = request.get_scan()
        forward = request.get_forward()
        accurate_query = request.get_accurate_query()
        from_time_nano_part = request.get_from_time_nano_part()
        to_time_nano_part = request.get_to_time_nano_part()

        return self.get_log(project, logstore, from_time, to_time, topic,
                            query, reverse, offset, size, power_sql, scan, forward, accurate_query, from_time_nano_part, to_time_nano_part)

    def get_log_all(self, project, logstore, from_time, to_time, topic=None,
                    query=None, reverse=False, offset=0, power_sql=False, accurate_query=True):
        """ Get logs from log service. will retry when incomplete.
        Unsuccessful operation will cause an LogException. different with `get_log` with size=-1,
        It will try to iteratively fetch all data every 100 items and yield them, in CLI, it could apply jmes filter to
        each batch and make it possible to fetch larger volume of data.

        :type project: string
        :param project: project name

        :type logstore: string
        :param logstore: logstore name

        :type from_time: int/string
        :param from_time: the begin timestamp or format of time in readable time like "%Y-%m-%d %H:%M:%S<time_zone>" e.g. "2018-01-02 12:12:10+8:00", also support human readable string, e.g. "1 hour ago", "now", "yesterday 0:0:0", refer to https://aliyun-log-cli.readthedocs.io/en/latest/tutorials/tutorial_human_readable_datetime.html

        :type to_time: int/string
        :param to_time: the end timestamp or format of time in readable time like "%Y-%m-%d %H:%M:%S<time_zone>" e.g. "2018-01-02 12:12:10+8:00", also support human readable string, e.g. "1 hour ago", "now", "yesterday 0:0:0", refer to https://aliyun-log-cli.readthedocs.io/en/latest/tutorials/tutorial_human_readable_datetime.html

        :type topic: string
        :param topic: topic name of logs, could be None

        :type query: string
        :param query: user defined query, could be None

        :type reverse: bool
        :param reverse: if reverse is set to true, the query will return the latest logs first, default is false

        :type offset: int
        :param offset: offset to start, by default is 0

        :type power_sql: bool
        :param power_sql: if power_sql is set to true, the query will run on enhanced sql mode

        :type accurate_query: bool
        :param accurate_query: if accurate_query is set to true, the query will global ordered time second mode

        :return: GetLogsResponse iterator

        :raise: LogException
        """
        while True:
            response = self.get_log(project, logstore, from_time, to_time, topic=topic,
                                    query=query, reverse=reverse, offset=offset, size=100, power_sql=power_sql, accurate_query=accurate_query)

            yield response

            count = response.get_count()
            offset += count

            if count == 0 or is_stats_query(query):
                break

    def get_log_all_v2(self, project, logstore, from_time, to_time, topic=None,
                    query=None, reverse=False, offset=0, power_sql=False, scan=False, forward=True):
        """ FOR PHRASE AND SCAN WHERE.
                Get logs from log service. will retry when incomplete.
                Unsuccessful operation will cause an LogException. different with `get_log` with size=-1,
                It will try to iteratively fetch all data every 100 items and yield them, in CLI, it could apply jmes filter to
                each batch and make it possible to fetch larger volume of data.

                :type project: string
                :param project: project name

                :type logstore: string
                :param logstore: logstore name

                :type from_time: int/string
                :param from_time: the begin timestamp or format of time in readable time like "%Y-%m-%d %H:%M:%S<time_zone>" e.g. "2018-01-02 12:12:10+8:00", also support human readable string, e.g. "1 hour ago", "now", "yesterday 0:0:0", refer to https://aliyun-log-cli.readthedocs.io/en/latest/tutorials/tutorial_human_readable_datetime.html

                :type to_time: int/string
                :param to_time: the end timestamp or format of time in readable time like "%Y-%m-%d %H:%M:%S<time_zone>" e.g. "2018-01-02 12:12:10+8:00", also support human readable string, e.g. "1 hour ago", "now", "yesterday 0:0:0", refer to https://aliyun-log-cli.readthedocs.io/en/latest/tutorials/tutorial_human_readable_datetime.html

                :type topic: string
                :param topic: topic name of logs, could be None

                :type query: string
                :param query: user defined query, could be None

                :type reverse: bool
                :param reverse: if reverse is set to true, the query will return the latest logs first, default is false

                :type offset: int
                :param offset: offset to start, by default is 0

                :type power_sql: bool
                :param power_sql: if power_sql is set to true, the query will run on enhanced sql mode

                :type scan: bool
                :param scan: if scan is set to true, the query will use scan mode

                :type forward: bool
                :param forward: only for scan query, if forward is set to true, the query will get next page, otherwise previous page

                :return: GetLogsResponse iterator

                :raise: LogException
                """
        while True:
            response = self.get_log(project, logstore, from_time, to_time, topic=topic,
                                    query=query, reverse=reverse, offset=offset, size=100, power_sql=power_sql,
                                    scan=scan, forward=forward)

            yield response

            count = response.get_count()
            query_mode = response.get_query_mode()
            scan_all = False
            if query_mode is GetLogsResponse.QueryMode.NORMAL or query_mode is GetLogsResponse.QueryMode.SCAN_SQL:
                offset += count
            else:
                scan_all = response.get_scan_all()
                if forward:
                    offset = response.get_end_offset()
                else:
                    offset = response.get_begin_offset()

            if count == 0 or is_stats_query(query) or scan_all:
                break

    def execute_logstore_sql(self, project, logstore, from_time, to_time, sql, power_sql):
        """ Execute SQL from log service.
        will retry DEFAULT_QUERY_RETRY_COUNT when incomplete.
        Unsuccessful operation will cause an LogException.

        :type project: string
        :param project: project name

        :type logstore: string
        :param logstore: logstore name

        :type from_time: int/string
        :param from_time: the begin timestamp or format of time in readable time like "%Y-%m-%d %H:%M:%S<time_zone>" e.g. "2018-01-02 12:12:10+8:00", also support human readable string, e.g. "1 hour ago", "now", "yesterday 0:0:0", refer to https://aliyun-log-cli.readthedocs.io/en/latest/tutorials/tutorial_human_readable_datetime.html

        :type to_time: int/string
        :param to_time: the end timestamp or format of time in readable time like "%Y-%m-%d %H:%M:%S<time_zone>" e.g. "2018-01-02 12:12:10+8:00", also support human readable string, e.g. "1 hour ago", "now", "yesterday 0:0:0", refer to https://aliyun-log-cli.readthedocs.io/en/latest/tutorials/tutorial_human_readable_datetime.html

        :type sql: string
        :param sql: user defined sql, must follow "Search|Analysis" syntax, refer to https://help.aliyun.com/document_detail/43772.html

        :type power_sql: bool
        :param power_sql: if power_sql is set to true, the query will run on enhanced sql mode

        :return: GetLogsResponse

        :raise: LogException
        """
        if not is_stats_query(sql):
            raise LogException("parameter sql invalid, please follow 'Search|Analysis' syntax, refer to "
                               "https://help.aliyun.com/document_detail/43772.html")
        return self.get_log(project, logstore, from_time, to_time, topic=None,
                            query=sql, reverse=False, offset=0, size=100, power_sql=power_sql)

    def execute_project_sql(self, project, sql, power_sql):
        """ Execute SQL from log service.
        Unsuccessful operation will cause an LogException.

        :type project: string
        :param project: project name

        :type sql: string
        :param sql: user defined sql, must follow SQL syntax, refer to https://help.aliyun.com/document_detail/63443.html

        :type power_sql: bool
        :param power_sql: if power_sql is set to true, the query will run on enhanced sql mode

        :return: GetLogsResponse

        :raise: LogException
        """
        request = GetProjectLogsRequest(project, sql, power_sql)
        return self.get_project_logs(request)

    def get_context_logs(self, project, logstore, pack_id, pack_meta, back_lines, forward_lines):
        """ Get context logs of specified log from log service.
        Unsuccessful operation will cause an LogException.

        :type project: string
        :param project: project name

        :type logstore: string
        :param logstore: logstore name

        :type pack_id: string
        :param pack_id: package ID of the start log, such as 895CEA449A52FE-1 ({hex prefix}-{hex sequence number}).

        :type pack_meta: string
        :param pack_meta: package meta of the start log, such as 0|MTU1OTI4NTExMjg3NTQ2MjQ3MQ==|2|1.

        :type back_lines: int
        :param back_lines: the number of logs to request backward, at most 100.

        :type forward_lines: int
        :param forward_lines: the number of logs to request forward, at most 100.

        :return: GetContextLogsResponse
        """
        ret = None
        for _c in xrange(DEFAULT_QUERY_RETRY_COUNT):
            headers = {}
            params = {'pack_id': pack_id,
                      'pack_meta': pack_meta,
                      'type': 'context_log',
                      'back_lines': back_lines,
                      'forward_lines': forward_lines}

            resource = "/logstores/" + logstore
            (resp, header) = self._send("GET", project, None, resource, params, headers)
            ret = GetContextLogsResponse(resp, header)
            if ret.is_completed():
                break

            time.sleep(DEFAULT_QUERY_RETRY_INTERVAL)

        return ret

    def get_project_logs(self, request):
        """ Get logs from log service.
        Unsuccessful operation will cause an LogException.

        :type request: GetProjectLogsRequest
        :param request: the GetProjectLogs request parameters class.

        :return: GetLogsResponse

        :raise: LogException
        """
        headers = {}
        params = {}
        if request.get_query() is not None:
            params['query'] = request.get_query()
        if request.get_power_sql() is not None:
            params['powerSql'] = request.get_power_sql()
        project = request.get_project()
        resource = "/logs"
        (resp, header) = self._send("GET", project, None, resource, params, headers)
        return GetLogsResponse._from_v1_resp(resp, header)

    def get_cursor(self, project_name, logstore_name, shard_id, start_time):
        """ Get cursor from log service for batch pull logs
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type logstore_name: string
        :param logstore_name: the logstore name

        :type shard_id: int
        :param shard_id: the shard id

        :type start_time: string/int
        :param start_time: the start time of cursor, e.g 1441093445 or "begin"/"end", or readable time like "%Y-%m-%d %H:%M:%S<time_zone>" e.g. "2018-01-02 12:12:10+8:00", also support human readable string, e.g. "1 hour ago", "now", "yesterday 0:0:0", refer to https://aliyun-log-cli.readthedocs.io/en/latest/tutorials/tutorial_human_readable_datetime.html

        :return: GetCursorResponse

        :raise: LogException
        """

        headers = {'Content-Type': 'application/json'}
        params = {'type': 'cursor',
                  'from': str(start_time) if start_time in ("begin", "end") else parse_timestamp(start_time)}

        resource = "/logstores/" + logstore_name + "/shards/" + str(shard_id)
        (resp, header) = self._send("GET", project_name, None, resource, params, headers)
        return GetCursorResponse(resp, header)

    def get_cursor_time(self, project_name, logstore_name, shard_id, cursor):
        """ Get cursor time from log service
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type logstore_name: string
        :param logstore_name: the logstore name

        :type shard_id: int
        :param shard_id: the shard id

        :type cursor: string
        :param cursor: the cursor to get its service receive time

        :return: GetCursorTimeResponse

        :raise: LogException
        """

        headers = {'Content-Type': 'application/json'}
        params = {'type': 'cursor_time', 'cursor': cursor}
        resource = "/logstores/" + logstore_name + "/shards/" + str(shard_id)

        (resp, header) = self._send("GET", project_name, None, resource, params, headers)
        return GetCursorTimeResponse(resp, header)

    @staticmethod
    def _get_cursor_as_int(cursor):
        return int(d64(cursor))

    def get_previous_cursor_time(self, project_name, logstore_name, shard_id, cursor, normalize=True):
        """ Get previous cursor time from log service.
        Note: normalize = true: if the cursor is out of range, it will be nornalized to nearest cursor
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type logstore_name: string
        :param logstore_name: the logstore name

        :type shard_id: int
        :param shard_id: the shard id

        :type cursor: string
        :param cursor: the cursor to get its service receive time

        :type normalize: bool
        :param normalize: fix the cursor or not if it's out of scope

        :return: GetCursorTimeResponse

        :raise: LogException
        """

        try:
            pre_cursor_int = self._get_cursor_as_int(cursor) - 1
            pre_cursor = e64(str(pre_cursor_int)).strip()
        except Exception:
            raise LogException("InvalidCursor", "Cursor {0} is invalid".format(cursor))

        try:
            return self.get_cursor_time(project_name, logstore_name, shard_id, pre_cursor)
        except LogException as ex:
            if normalize and ex.get_error_code() == "InvalidCursor":
                ret = self.get_begin_cursor(project_name, logstore_name, shard_id)
                begin_cursor_int = self._get_cursor_as_int(ret.get_cursor())

                if pre_cursor_int < begin_cursor_int:
                    return self.get_cursor_time(project_name, logstore_name, shard_id, e64(str(begin_cursor_int)))

                ret = self.get_end_cursor(project_name, logstore_name, shard_id)
                end_cursor_int = self._get_cursor_as_int(ret.get_cursor())

                if pre_cursor_int > end_cursor_int:
                    return self.get_cursor_time(project_name, logstore_name, shard_id, e64(str(end_cursor_int)))

            raise ex

    def get_begin_cursor(self, project_name, logstore_name, shard_id):
        """ Get begin cursor from log service for batch pull logs
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type logstore_name: string
        :param logstore_name: the logstore name

        :type shard_id: int
        :param shard_id: the shard id

        :return: GetLogsResponse

        :raise: LogException
        """
        return self.get_cursor(project_name, logstore_name, shard_id, "begin")

    def get_end_cursor(self, project_name, logstore_name, shard_id):
        """ Get end cursor from log service for batch pull logs
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type logstore_name: string
        :param logstore_name: the logstore name

        :type shard_id: int
        :param shard_id: the shard id

        :return: GetLogsResponse

        :raise: LogException
        """
        return self.get_cursor(project_name, logstore_name, shard_id, "end")

    def pull_logs(self, project_name, logstore_name, shard_id, cursor, count=None, end_cursor=None, compress=None, query=None):
        """ batch pull log data from log service
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type logstore_name: string
        :param logstore_name: the logstore name

        :type shard_id: int
        :param shard_id: the shard id

        :type cursor: string
        :param cursor: the start to cursor to get data

        :type count: int
        :param count: the required pull log package count, default 1000 packages

        :type end_cursor: string
        :param end_cursor: the end cursor position to get data

        :type compress: boolean
        :param compress: if use zip compress for transfer data, default is True

        :type query: string
        :param query: the SPL query, such as *| where a = 'xxx'

        :return: PullLogResponse

        :raise: LogException
        """

        headers = {}
        if compress is None or compress:
            if lz4_available:
                headers['Accept-Encoding'] = 'lz4'
            else:
                headers['Accept-Encoding'] = 'gzip'
        else:
            headers['Accept-Encoding'] = ''

        headers['Accept'] = 'application/x-protobuf'

        params = {}
        resource = "/logstores/" + logstore_name + "/shards/" + str(shard_id)
        params['type'] = 'log'
        params['cursor'] = cursor
        count = count or 1000
        params['count'] = str(count)
        if isinstance(query, str) and len(query.strip()) <= 0:
            query = None
        if query:
            params['pullMode'] = "scan_on_stream"
            params['query'] = query
        if end_cursor:
            params['end_cursor'] = end_cursor
        (resp, header) = self._send("GET", project_name, None, resource, params, headers, "binary")
        raw_size = int(Util.h_v_t(header, 'x-log-bodyrawsize'))
        if raw_size <= 0:
            return PullLogResponse(None, header)
        compress_type = Util.h_v_td(header, 'x-log-compresstype', '').lower()
        if compress_type == 'lz4':
            if lz4_available:
                raw_data = lz_decompress(raw_size, resp)
                return PullLogResponse(raw_data, header)
            else:
                raise LogException("ClientHasNoLz4", "There's no Lz4 lib available to decompress the response", resp_header=header, resp_body=resp)
        elif compress_type in ('gzip', 'deflate'):
            raw_data = zlib.decompress(resp)
            return PullLogResponse(raw_data, header)
        else:
            return PullLogResponse(resp, header)

    def pull_log(self, project_name, logstore_name, shard_id, from_time, to_time, batch_size=None, compress=None, query=None):
        """ batch pull log data from log service using time-range
        Unsuccessful operation will cause an LogException. the time parameter means the time when server receives the logs

        :type project_name: string
        :param project_name: the Project name

        :type logstore_name: string
        :param logstore_name: the logstore name

        :type shard_id: int
        :param shard_id: the shard id

        :type from_time: string/int
        :param from_time: curosr value, could be begin, timestamp or readable time in readable time like "%Y-%m-%d %H:%M:%S<time_zone>" e.g. "2018-01-02 12:12:10+8:00", also support human readable string, e.g. "1 hour ago", "now", "yesterday 0:0:0", refer to https://aliyun-log-cli.readthedocs.io/en/latest/tutorials/tutorial_human_readable_datetime.html

        :type to_time: string/int
        :param to_time: curosr value, could be begin, timestamp or readable time in readable time like "%Y-%m-%d %H:%M:%S<time_zone>" e.g. "2018-01-02 12:12:10+8:00", also support human readable string, e.g. "1 hour ago", "now", "yesterday 0:0:0", refer to https://aliyun-log-cli.readthedocs.io/en/latest/tutorials/tutorial_human_readable_datetime.html

        :type batch_size: int
        :param batch_size: batch size to fetch the data in each iteration. by default it's 1000

        :type compress: bool
        :param compress: if use compression, by default it's True

        :type query: string
        :param query: the SPL query, such as *| where a = 'xxx'

        :return: PullLogResponse

        :raise: LogException
        """
        begin_cursor = self.get_cursor(project_name, logstore_name, shard_id, from_time).get_cursor()
        end_cursor = self.get_cursor(project_name, logstore_name, shard_id, to_time).get_cursor()

        while True:
            res = self.pull_logs(project_name, logstore_name, shard_id, begin_cursor,
                                 count=batch_size, end_cursor=end_cursor, compress=compress, query=query)

            yield res
            if res.get_log_count() <= 0:
                break

            begin_cursor = res.get_next_cursor()

    def pull_log_dump(self, project_name, logstore_name, from_time, to_time, file_path, batch_size=None,
                      compress=None, encodings=None, shard_list=None, no_escape=None, query=None):
        """ dump all logs seperatedly line into file_path, file_path, the time parameters are log received time on server side.

        :type project_name: string
        :param project_name: the Project name

        :type logstore_name: string
        :param logstore_name: the logstore name

        :type from_time: string/int
        :param from_time: curosr value, could be begin, timestamp or readable time in readable time like "%Y-%m-%d %H:%M:%S<time_zone>" e.g. "2018-01-02 12:12:10+8:00", also support human readable string, e.g. "1 hour ago", "now", "yesterday 0:0:0", refer to https://aliyun-log-cli.readthedocs.io/en/latest/tutorials/tutorial_human_readable_datetime.html

        :type to_time: string/int
        :param to_time: curosr value, could be begin, timestamp or readable time in readable time like "%Y-%m-%d %H:%M:%S<time_zone>" e.g. "2018-01-02 12:12:10+8:00", also support human readable string, e.g. "1 hour ago", "now", "yesterday 0:0:0", refer to https://aliyun-log-cli.readthedocs.io/en/latest/tutorials/tutorial_human_readable_datetime.html

        :type file_path: string
        :param file_path: file path with {} for shard id. e.g. "/data/dump_{}.data", {} will be replaced with each partition.

        :type batch_size: int
        :param batch_size: batch size to fetch the data in each iteration. by default it's 500

        :type compress: bool
        :param compress: if use compression, by default it's True

        :type encodings: string list
        :param encodings: encoding like ["utf8", "latin1"] etc to dumps the logs in json format to file. default is ["utf8",]

        :type shard_list: string
        :param shard_list: shard number list. could be comma seperated list or range: 1,20,31-40

        :type no_escape: bool
        :param no_escape: if not_escape the non-ANSI, default is to escape, set it to True if don't want it.

        :type query: string
        :param query: the SPL query, such as *| where a = 'xxx'

        :return: LogResponse {"total_count": 30, "files": {'file_path_1': 10, "file_path_2": 20} })

        :raise: LogException
        """
        file_path = file_path.replace("{}", "{0}")
        if "{0}" not in file_path:
            file_path += "{0}"

        return pull_log_dump(self, project_name, logstore_name, from_time, to_time, file_path,
                             batch_size=batch_size, compress=compress, encodings=encodings,
                             shard_list=shard_list, no_escape=no_escape, query=query)

    def create_logstore(self, project_name, logstore_name,
                        ttl=30,
                        shard_count=2,
                        enable_tracking=False,
                        append_meta=False,
                        auto_split=True,
                        max_split_shard=64,
                        preserve_storage=False,
                        encrypt_conf=None,
                        telemetry_type='',
                        hot_ttl=-1,
                        mode = None,
                        infrequent_access_ttl=-1
                        ):
        """ create log store
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type logstore_name: string
        :param logstore_name: the logstore name

        :type ttl: int
        :param ttl: the life cycle of log in the logstore in days, default 30, up to 3650

        :type shard_count: int
        :param shard_count: the shard count of the logstore to create, default 2

        :type enable_tracking: bool
        :param enable_tracking: enable web tracking, default is False

        :type append_meta: bool
        :param append_meta: allow to append meta info (server received time and IP for external IP to each received log)

        :type auto_split: bool
        :param auto_split: auto split shard, max_split_shard will be 64 by default is True

        :type max_split_shard: int
        :param max_split_shard: max shard to split, up to 256

        :type preserve_storage: bool
        :param preserve_storage: if always persist data, TTL will be ignored.

        :type encrypt_conf: dict
        :param encrypt_conf :  following is a sample
        +       {
+                    "enable" : True/False,              # required
+                    "encrypt_type" : "default",         # required, default encrypt alogrithm only currently
+                    "user_cmk_info" :                   # optional, if 'user_cmk_info' is set, use byok cmk key, otherwise use sls system cmk key
+                    {
+                        "cmk_key_id" :                  # the cmk key used to generate data encrypt key
+                        "arn" :                         # arn to grant sls service to get/generate data encrypt key in kms
+                        "region_id" :                   # the region id of cmk_key_id
+                    }
+                }
        :type telemetry_type: string
        :param telemetry_type: the Telemetry type

        :type mode: string
        :param mode: type of logstore, can be choose between lite and standard, default value standard

        :type infrequent_access_ttl: int
        :param infrequent_access_ttl: infrequent access storage time

        :type hot_ttl: int
        :param hot_ttl: the life cycle of hot storage,[0-hot_ttl]is hot storage, (hot_ttl-ttl] is warm storage, if hot_ttl=-1, it means [0-ttl]is all hot storage

        :return: CreateLogStoreResponse

        :raise: LogException
        """
        if preserve_storage:
            ttl = 3650

        params = {}
        resource = "/logstores"
        headers = {"x-log-bodyrawsize": '0', "Content-Type": "application/json"}
        body = {"logstoreName": logstore_name, "ttl": int(ttl), "shardCount": int(shard_count),
                "enable_tracking": enable_tracking,
                "autoSplit": auto_split,
                "maxSplitShard": max_split_shard,
                "appendMeta": append_meta,
                "telemetryType": telemetry_type
                }
        if hot_ttl !=-1:
            body['hot_ttl'] = hot_ttl
        if encrypt_conf != None:
            body["encrypt_conf"] = encrypt_conf
        if mode != None:
            body["mode"] = mode
        if infrequent_access_ttl >= 0:
            body["infrequentAccessTTL"] = infrequent_access_ttl

        body_str = six.b(json.dumps(body))

        try:
            (resp, header) = self._send("POST", project_name, body_str, resource, params, headers)
        except LogException as ex:
            if ex.get_error_code() == "LogStoreInfoInvalid" and ex.get_error_message() == "redundant key exist in json":
                logger.warning("LogStoreInfoInvalid, will retry with basic parameters. detail: {0}".format(ex))
                body = {"logstoreName": logstore_name, "ttl": int(ttl), "shardCount": int(shard_count),
                        "enable_tracking": enable_tracking }

                body_str = six.b(json.dumps(body))

                (resp, header) = self._send("POST", project_name, body_str, resource, params, headers)
            else:
                raise

        return CreateLogStoreResponse(header, resp)

    def delete_logstore(self, project_name, logstore_name):
        """ delete log store
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type logstore_name: string
        :param logstore_name: the logstore name

        :return: DeleteLogStoreResponse

        :raise: LogException
        """
        headers = {}
        params = {}
        resource = "/logstores/" + logstore_name
        (resp, header) = self._send("DELETE", project_name, None, resource, params, headers)
        return DeleteLogStoreResponse(header, resp)

    def get_logstore(self, project_name, logstore_name):
        """ get the logstore meta info
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type logstore_name: string
        :param logstore_name: the logstore name

        :return: GetLogStoreResponse

        :raise: LogException
        """

        headers = {}
        params = {}
        resource = "/logstores/" + logstore_name
        (resp, header) = self._send("GET", project_name, None, resource, params, headers)
        return GetLogStoreResponse(resp, header)

    def update_logstore(self, project_name, logstore_name, ttl=None, enable_tracking=None, shard_count=None,
                        append_meta=None,
                        auto_split=None,
                        max_split_shard=None,
                        preserve_storage=None,
                        encrypt_conf=None,
                        hot_ttl=-1,
                        mode = None,
                        telemetry_type=None,
                        infrequent_access_ttl=-1
                        ):
        """
        update the logstore meta info
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type logstore_name: string
        :param logstore_name: the logstore name

        :type ttl: int
        :param ttl: the life cycle of log in the logstore in days

        :type hot_ttl: int
        :param hot_ttl: the life cycle of hot storage,[0-hot_ttl]is hot storage, (hot_ttl-ttl] is warm storage, if hot_ttl=-1, it means [0-ttl]is all hot storage

        :type enable_tracking: bool
        :param enable_tracking: enable web tracking

        :type shard_count: int
        :param shard_count: deprecated, the shard count could only be updated by split & merge

        :type append_meta: bool
        :param append_meta: allow to append meta info (server received time and IP for external IP to each received log)

        :type auto_split: bool
        :param auto_split: auto split shard, max_split_shard will be 64 by default is True

        :type max_split_shard: int
        :param max_split_shard: max shard to split, up to 256

        :type preserve_storage: bool
        :param preserve_storage: if always persist data, TTL will be ignored.

        :type encrypt_conf: dict
        :param encrypt_conf :  following is a sample
+                {
+                    "enable" : True/False,              # required
+                    "encrypt_type" : "default",         # required, default encrypt alogrithm only currently
+                    "user_cmk_info" :                   # optional, if 'user_cmk_info' is set, use byok cmk key, otherwise use sls system cmk key
+                    {
+                        "cmk_key_id" :                  # the cmk key used to generate data encrypt key
+                        "arn" :                         # arn to grant sls service to get/generate data encrypt key in kms
+                        "region_id" :                   # the region id of cmk_key_id
+                    }
+                }

        :type mode: string
        :param mode: type of logstore, can be choose between lite and standard, default value standard
        :type telemetry_type: string
        :param telemetry_type: the Telemetry type

        :type infrequent_access_ttl: int
        :param infrequent_access_ttl: infrequent access storage time

        :return: UpdateLogStoreResponse

        :raise: LogException
        """

        res = self.get_logstore(project_name, logstore_name)
        shard_count = res.get_shard_count()

        if enable_tracking is None:
            enable_tracking = res.get_enable_tracking()
        if preserve_storage is None and ttl is None:
            preserve_storage = res.preserve_storage
        if ttl is None:
            ttl = res.get_ttl()
        if auto_split is None:
            auto_split = res.auto_split
        if append_meta is None:
            append_meta = res.append_meta
        if max_split_shard is None:
            max_split_shard = res.max_split_shard

        if preserve_storage:
            ttl = 3650

        headers = {"x-log-bodyrawsize": '0', "Content-Type": "application/json"}
        params = {}
        resource = "/logstores/" + logstore_name
        body = {
            "logstoreName": logstore_name, "ttl": int(ttl), "enable_tracking": enable_tracking,
            "shardCount": shard_count,
            "autoSplit": auto_split,
            "maxSplitShard": max_split_shard,
            "appendMeta": append_meta
        }
        if hot_ttl !=-1:
            body['hot_ttl'] = hot_ttl
        if encrypt_conf != None:
            body["encrypt_conf"] = encrypt_conf
        if mode != None:
            body['mode'] = mode
        if telemetry_type != None:
            body["telemetryType"] = telemetry_type
        if infrequent_access_ttl >= 0:
            body["infrequentAccessTTL"] = infrequent_access_ttl
        body_str = six.b(json.dumps(body))
        try:
            (resp, header) = self._send("PUT", project_name, body_str, resource, params, headers)
        except LogException as ex:
            if ex.get_error_code() == "LogStoreInfoInvalid" and ex.get_error_message() == "redundant key exist in json":
                logger.warning("LogStoreInfoInvalid, will retry with basic parameters. detail: {0}".format(ex))
                body = { "logstoreName": logstore_name, "ttl": int(ttl), "enable_tracking": enable_tracking,
                         "shardCount": shard_count }

                body_str = six.b(json.dumps(body))

                (resp, header) = self._send("PUT", project_name, body_str, resource, params, headers)
            else:
                raise

        return UpdateLogStoreResponse(header, resp)

    def list_logstore(self, project_name, logstore_name_pattern=None, offset=0, size=100):
        """ list the logstore in a projectListLogStoreResponse
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type logstore_name_pattern: string
        :param logstore_name_pattern: the sub name logstore, used for the server to return logstore names contain this sub name

        :type offset: int
        :param offset: the offset of all the matched names

        :type size: int
        :param size: the max return names count, -1 means all

        :return: ListLogStoreResponse

        :raise: LogException
        """

        # need to use extended method to get more
        if int(size) == -1 or int(size) > MAX_LIST_PAGING_SIZE:
            return list_more(self.list_logstore, int(offset), int(size), MAX_LIST_PAGING_SIZE,
                             project_name, logstore_name_pattern)

        headers = {}
        params = {}
        resource = "/logstores"
        if logstore_name_pattern is not None:
            params['logstoreName'] = logstore_name_pattern
        params['offset'] = str(offset)
        params['size'] = str(size)
        (resp, header) = self._send("GET", project_name, None, resource, params, headers)
        return ListLogStoreResponse(resp, header)

    def create_external_store(self, project_name, config):
        """ create log store
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type config : ExternalStoreConfigBase
        :param config :external store config


        :return: CreateExternalStoreResponse

        :raise: LogException
        """
        params = {}
        resource = "/externalstores"
        headers = {"x-log-bodyrawsize": '0', "Content-Type": "application/json"}

        body_str = six.b(json.dumps(config.to_json()))

        (resp, header) = self._send("POST", project_name, body_str, resource, params, headers)
        return CreateExternalStoreResponse(header, resp)

    def delete_external_store(self, project_name, store_name):
        """ delete log store
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type store_name: string
        :param store_name: the external store name

        :return: DeleteExternalStoreResponse

        :raise: LogException
        """
        headers = {}
        params = {}
        resource = "/externalstores/" + store_name
        (resp, header) = self._send("DELETE", project_name, None, resource, params, headers)
        return DeleteExternalStoreResponse(header, resp)

    def get_external_store(self, project_name, store_name):
        """ get the logstore meta info
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type store_name: string
        :param store_name: the logstore name

        :return: GetLogStoreResponse

        :raise: LogException
        """

        headers = {}
        params = {}
        resource = "/externalstores/" + store_name
        (resp, header) = self._send("GET", project_name, None, resource, params, headers)

        # add storeName if not existing
        if 'externalStoreName' not in resp:
            resp['externalStoreName'] = store_name

        return GetExternalStoreResponse(resp, header)

    def update_external_store(self, project_name, config):
        """
        update the logstore meta info
        Unsuccessful operation will cause an LogException.

        :type config: ExternalStoreConfigBase
        :param config : external store config

        :return: UpdateExternalStoreResponse

        :raise: LogException
        """

        headers = {"x-log-bodyrawsize": '0', "Content-Type": "application/json"}
        params = {}
        resource = "/externalstores/" + config.externalStoreName
        body_str = six.b(json.dumps(config.to_json()))
        (resp, header) = self._send("PUT", project_name, body_str, resource, params, headers)
        return UpdateExternalStoreResponse(header, resp)

    def list_external_store(self, project_name, external_store_name_pattern=None, offset=0, size=100):
        """ list the logstore in a projectListLogStoreResponse
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type logstore_name_pattern: string
        :param logstore_name_pattern: the sub name logstore, used for the server to return logstore names contain this sub name

        :type offset: int
        :param offset: the offset of all the matched names

        :type size: int
        :param size: the max return names count, -1 means all

        :return: ListLogStoreResponse

        :raise: LogException
        """

        # need to use extended method to get more
        if int(size) == -1 or int(size) > MAX_LIST_PAGING_SIZE:
            return list_more(self.list_external_store, int(offset), int(size), MAX_LIST_PAGING_SIZE,
                             project_name, external_store_name_pattern)

        headers = {}
        params = {}
        resource = "/externalstores"
        if external_store_name_pattern is not None:
            params['externalStoreName'] = external_store_name_pattern
        params['offset'] = str(offset)
        params['size'] = str(size)
        (resp, header) = self._send("GET", project_name, None, resource, params, headers)
        return ListExternalStoreResponse(resp, header)

    def list_shards(self, project_name, logstore_name):
        """ list the shard meta of a logstore
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type logstore_name: string
        :param logstore_name: the logstore name

        :return: ListShardResponse

        :raise: LogException
        """
        headers = {}
        params = {}
        resource = "/logstores/" + logstore_name + "/shards"
        (resp, header) = self._send("GET", project_name, None, resource, params, headers)
        return ListShardResponse(resp, header)

    def split_shard(self, project_name, logstore_name, shardId, split_hash):
        """ split a  readwrite shard into two shards
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type logstore_name: string
        :param logstore_name: the logstore name

        :type shardId: int
        :param shardId: the shard id

        :type split_hash: string
        :param split_hash: the internal hash between the shard begin and end hash

        :return: ListShardResponse

        :raise: LogException
        """

        headers = {}
        params = {"action": "split", "key": split_hash}
        resource = "/logstores/" + logstore_name + "/shards/" + str(shardId)
        (resp, header) = self._send("POST", project_name, None, resource, params, headers)
        return ListShardResponse(resp, header)

    def merge_shard(self, project_name, logstore_name, shardId):
        """ split two adjacent  readwrite hards into one shards
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type logstore_name: string
        :param logstore_name: the logstore name

        :type shardId: int
        :param shardId: the shard id of the left shard, server will determine the right adjacent shardId

        :return: ListShardResponse

        :raise: LogException
        """
        headers = {}
        params = {"action": "merge"}
        resource = "/logstores/" + logstore_name + "/shards/" + str(shardId)
        (resp, header) = self._send("POST", project_name, None, resource, params, headers)
        return ListShardResponse(resp, header)

    def delete_shard(self, project_name, logstore_name, shardId):
        """ delete a readonly shard
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type logstore_name: string
        :param logstore_name: the logstore name

        :type shardId: int
        :param shardId: the read only shard id

        :return: ListShardResponse

        :raise: LogException
        """
        headers = {}
        params = {}
        resource = "/logstores/" + logstore_name + "/shards/" + str(shardId)
        (resp, header) = self._send("DELETE", project_name, None, resource, params, headers)
        return DeleteShardResponse(header, resp)

    def create_index(self, project_name, logstore_name, index_detail):
        """ create index for a logstore
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type logstore_name: string
        :param logstore_name: the logstore name

        :type index_detail: IndexConfig
        :param index_detail: the index config detail used to create index

        :return: CreateIndexResponse

        :raise: LogException
        """
        headers = {}
        params = {}
        resource = "/logstores/" + logstore_name + "/index"
        headers['Content-Type'] = 'application/json'
        body = six.b(json.dumps(index_detail.to_json()))
        headers['x-log-bodyrawsize'] = str(len(body))

        (resp, header) = self._send("POST", project_name, body, resource, params, headers)
        return CreateIndexResponse(header, resp)

    def update_index(self, project_name, logstore_name, index_detail):
        """ update index for a logstore
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type logstore_name: string
        :param logstore_name: the logstore name

        :type index_detail: IndexConfig
        :param index_detail: the index config detail used to update index

        :return: UpdateIndexResponse

        :raise: LogException
        """

        headers = {}
        params = {}
        resource = "/logstores/" + logstore_name + "/index"
        headers['Content-Type'] = 'application/json'
        body = six.b(json.dumps(index_detail.to_json()))
        headers['x-log-bodyrawsize'] = str(len(body))

        (resp, header) = self._send("PUT", project_name, body, resource, params, headers)
        return UpdateIndexResponse(header, resp)

    def delete_index(self, project_name, logstore_name):
        """ delete index of a logstore
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type logstore_name: string
        :param logstore_name: the logstore name

        :return: DeleteIndexResponse

        :raise: LogException
        """

        headers = {}
        params = {}
        resource = "/logstores/" + logstore_name + "/index"
        (resp, header) = self._send("DELETE", project_name, None, resource, params, headers)
        return DeleteIndexResponse(header, resp)

    def get_index_config(self, project_name, logstore_name):
        """ get index config detail of a logstore
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type logstore_name: string
        :param logstore_name: the logstore name

        :return: GetIndexResponse

        :raise: LogException
        """

        headers = {}
        params = {}
        resource = "/logstores/" + logstore_name + "/index"
        (resp, header) = self._send("GET", project_name, None, resource, params, headers)
        return GetIndexResponse(resp, header)

    def create_logtail_config(self, project_name, config_detail):
        """ create logtail config in a project
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type config_detail: LogtailConfigGenerator or SeperatorFileConfigDetail or SimpleFileConfigDetail or FullRegFileConfigDetail or JsonFileConfigDetail or ApsaraFileConfigDetail or SyslogConfigDetail or CommonRegLogConfigDetail
        :param config_detail: the logtail config detail info, use `LogtailConfigGenerator.from_json` to generate config: SeperatorFileConfigDetail or SimpleFileConfigDetail or FullRegFileConfigDetail or JsonFileConfigDetail or ApsaraFileConfigDetail or SyslogConfigDetail, Note: CommonRegLogConfigDetail is deprecated.

        :return: CreateLogtailConfigResponse

        :raise: LogException
        """

        if config_detail.logstore_name:
            # try to verify if the logstore exists or not.
            self.get_logstore(project_name, config_detail.logstore_name)

        headers = {}
        params = {}
        resource = "/configs"
        headers['Content-Type'] = 'application/json'
        body = six.b(json.dumps(config_detail.to_json()))
        headers['x-log-bodyrawsize'] = str(len(body))
        (resp, headers) = self._send("POST", project_name, body, resource, params, headers)
        return CreateLogtailConfigResponse(headers, resp)

    def update_logtail_config(self, project_name, config_detail):
        """ update logtail config in a project
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type config_detail: LogtailConfigGenerator or SeperatorFileConfigDetail or SimpleFileConfigDetail or FullRegFileConfigDetail or JsonFileConfigDetail or ApsaraFileConfigDetail or SyslogConfigDetail or CommonRegLogConfigDetail
        :param config_detail: the logtail config detail info, use `LogtailConfigGenerator.from_json` to generate config: SeperatorFileConfigDetail or SimpleFileConfigDetail or FullRegFileConfigDetail or JsonFileConfigDetail or ApsaraFileConfigDetail or SyslogConfigDetail

        :return: UpdateLogtailConfigResponse

        :raise: LogException
        """

        headers = {}
        params = {}
        resource = "/configs/" + config_detail.config_name
        headers['Content-Type'] = 'application/json'
        body = six.b(json.dumps(config_detail.to_json()))
        headers['x-log-bodyrawsize'] = str(len(body))
        (resp, headers) = self._send("PUT", project_name, body, resource, params, headers)
        return UpdateLogtailConfigResponse(headers, resp)

    def delete_logtail_config(self, project_name, config_name):
        """ delete logtail config in a project
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type config_name: string
        :param config_name: the logtail config name

        :return: DeleteLogtailConfigResponse

        :raise: LogException
        """

        headers = {}
        params = {}
        resource = "/configs/" + config_name
        (resp, headers) = self._send("DELETE", project_name, None, resource, params, headers)
        return DeleteLogtailConfigResponse(headers, resp)

    def get_logtail_config(self, project_name, config_name):
        """ get logtail config in a project
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type config_name: string
        :param config_name: the logtail config name

        :return: GetLogtailConfigResponse

        :raise: LogException
        """
        headers = {}
        params = {}
        resource = "/configs/" + config_name
        (resp, headers) = self._send("GET", project_name, None, resource, params, headers)
        return GetLogtailConfigResponse(resp, headers)

    def list_logtail_config(self, project_name, logstore=None, config=None, offset=0, size=100):
        """ list logtail config name in a project
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type logstore: string
        :param logstore: logstore name to filter related config

        :type config: string
        :param config: config name to filter related config

        :type offset: int
        :param offset: the offset of all config names

        :type size: int
        :param size: the max return names count, -1 means all

        :return: ListLogtailConfigResponse

        :raise: LogException
        """
        # need to use extended method to get more
        if int(size) == -1 or int(size) > MAX_LIST_PAGING_SIZE:
            return list_more(self.list_logtail_config, int(offset), int(size), MAX_LIST_PAGING_SIZE, project_name, logstore, config)

        headers = {}
        params = {}
        resource = "/configs"
        params['offset'] = str(offset)
        params['size'] = str(size)
        if logstore:
            params['logstoreName'] = logstore
        if config:
            params['configName'] = config
        (resp, header) = self._send("GET", project_name, None, resource, params, headers)
        return ListLogtailConfigResponse(resp, header)

    def create_machine_group(self, project_name, group_detail):
        """ create machine group in a project
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type group_detail: MachineGroupDetail
        :param group_detail: the machine group detail config

        :return: CreateMachineGroupResponse

        :raise: LogException
        """

        headers = {}
        params = {}
        resource = "/machinegroups"
        headers['Content-Type'] = 'application/json'
        body = six.b(json.dumps(group_detail.to_json()))
        headers['x-log-bodyrawsize'] = str(len(body))
        (resp, headers) = self._send("POST", project_name, body, resource, params, headers)
        return CreateMachineGroupResponse(headers, resp)

    def delete_machine_group(self, project_name, group_name):
        """ delete machine group in a project
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type group_name: string
        :param group_name: the group name

        :return: DeleteMachineGroupResponse

        :raise: LogException
        """

        headers = {}
        params = {}
        resource = "/machinegroups/" + group_name
        (resp, headers) = self._send("DELETE", project_name, None, resource, params, headers)
        return DeleteMachineGroupResponse(headers, resp)

    def update_machine_group(self, project_name, group_detail):
        """ update machine group in a project
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type group_detail: MachineGroupDetail
        :param group_detail: the machine group detail config

        :return: UpdateMachineGroupResponse

        :raise: LogException
        """

        headers = {}
        params = {}
        resource = "/machinegroups/" + group_detail.group_name
        headers['Content-Type'] = 'application/json'
        body = six.b(json.dumps(group_detail.to_json()))
        headers['x-log-bodyrawsize'] = str(len(body))
        (resp, headers) = self._send("PUT", project_name, body, resource, params, headers)
        return UpdateMachineGroupResponse(headers, resp)

    def get_machine_group(self, project_name, group_name):
        """ get machine group in a project
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type group_name: string
        :param group_name: the group name to get

        :return: GetMachineGroupResponse

        :raise: LogException
        """

        headers = {}
        params = {}
        resource = "/machinegroups/" + group_name
        (resp, headers) = self._send("GET", project_name, None, resource, params, headers)
        return GetMachineGroupResponse(resp, headers)

    def list_machine_group(self, project_name, offset=0, size=100):
        """ list machine group names in a project
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type offset: int
        :param offset: the offset of all group name

        :type size: int
        :param size: the max return names count, -1 means all

        :return: ListMachineGroupResponse

        :raise: LogException
        """

        # need to use extended method to get more
        if int(size) == -1 or int(size) > MAX_LIST_PAGING_SIZE:
            return list_more(self.list_machine_group, int(offset), int(size), MAX_LIST_PAGING_SIZE, project_name)

        headers = {}
        params = {}
        resource = "/machinegroups"
        params['offset'] = str(offset)
        params['size'] = str(size)
        (resp, header) = self._send("GET", project_name, None, resource, params, headers)
        return ListMachineGroupResponse(resp, header)

    def list_machines(self, project_name, group_name, offset=0, size=100):
        """ list machines in a machine group
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type group_name: string
        :param group_name: the group name to list

        :type offset: int
        :param offset: the offset of all group name

        :type size: int
        :param size: the max return names count, -1 means all

        :return: ListMachinesResponse

        :raise: LogException
        """

        # need to use extended method to get more
        if int(size) == -1 or int(size) > MAX_LIST_PAGING_SIZE:
            return list_more(self.list_machines, int(offset), int(size), MAX_LIST_PAGING_SIZE, project_name, group_name)

        headers = {}
        params = {}
        resource = "/machinegroups/" + group_name + "/machines"
        params['offset'] = str(offset)
        params['size'] = str(size)
        (resp, header) = self._send("GET", project_name, None, resource, params, headers)
        return ListMachinesResponse(resp, header)

    def apply_config_to_machine_group(self, project_name, config_name, group_name):
        """ apply a logtail config to a machine group
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type config_name: string
        :param config_name: the logtail config name to apply

        :type group_name: string
        :param group_name: the machine group name

        :return: ApplyConfigToMachineGroupResponse

        :raise: LogException
        """
        headers = {}
        params = {}
        resource = "/machinegroups/" + group_name + "/configs/" + config_name
        (resp, header) = self._send("PUT", project_name, None, resource, params, headers)
        return ApplyConfigToMachineGroupResponse(header, resp)

    def remove_config_to_machine_group(self, project_name, config_name, group_name):
        """ remove a logtail config to a machine group
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type config_name: string
        :param config_name: the logtail config name to apply

        :type group_name: string
        :param group_name: the machine group name

        :return: RemoveConfigToMachineGroupResponse

        :raise: LogException
        """
        headers = {}
        params = {}
        resource = "/machinegroups/" + group_name + "/configs/" + config_name
        (resp, header) = self._send("DELETE", project_name, None, resource, params, headers)
        return RemoveConfigToMachineGroupResponse(header, resp)

    def get_machine_group_applied_configs(self, project_name, group_name):
        """ get the logtail config names applied in a machine group
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type group_name: string
        :param group_name: the group name list

        :return: GetMachineGroupAppliedConfigResponse

        :raise: LogException
        """

        headers = {}
        params = {}
        resource = "/machinegroups/" + group_name + "/configs"
        (resp, header) = self._send("GET", project_name, None, resource, params, headers)
        return GetMachineGroupAppliedConfigResponse(resp, header)

    def get_config_applied_machine_groups(self, project_name, config_name):
        """ get machine group names where the logtail config applies to
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type config_name: string
        :param config_name: the logtail config name used to apply

        :return: GetConfigAppliedMachineGroupsResponse

        :raise: LogException
        """

        headers = {}
        params = {}
        resource = "/configs/" + config_name + "/machinegroups"
        (resp, header) = self._send("GET", project_name, None, resource, params, headers)
        return GetConfigAppliedMachineGroupsResponse(resp, header)

    def get_shipper_tasks(self, project_name, logstore_name, shipper_name, start_time, end_time, status_type='',
                          offset=0, size=100):
        """ get  odps/oss shipper tasks in a certain time range
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type logstore_name: string
        :param logstore_name: the logstore name

        :type shipper_name: string
        :param shipper_name: the shipper name

        :type start_time: int
        :param start_time: the start timestamp

        :type end_time: int
        :param end_time: the end timestamp

        :type status_type: string
        :param status_type: support one of ['', 'fail', 'success', 'running'] , if the status_type = '' , return all kinds of status type

        :type offset: int
        :param offset: the begin task offset

        :type size: int
        :param size: the needed tasks count, -1 means all

        :return: GetShipperTasksResponse

        :raise: LogException
        """
        # need to use extended method to get more
        if int(size) == -1 or int(size) > MAX_LIST_PAGING_SIZE:
            return list_more(self.get_shipper_tasks, int(offset), int(size), MAX_LIST_PAGING_SIZE,
                             project_name, logstore_name, shipper_name, start_time, end_time, status_type)

        headers = {}
        params = {"from": str(int(start_time)),
                  "to": str(int(end_time)),
                  "status": status_type,
                  "offset": str(int(offset)),
                  "size": str(int(size))}

        resource = "/logstores/" + logstore_name + "/shipper/" + shipper_name + "/tasks"
        (resp, header) = self._send("GET", project_name, None, resource, params, headers)
        return GetShipperTasksResponse(resp, header)

    def retry_shipper_tasks(self, project_name, logstore_name, shipper_name, task_list):
        """ retry failed tasks , only the failed task can be retried
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type logstore_name: string
        :param logstore_name: the logstore name

        :type shipper_name: string
        :param shipper_name: the shipper name

        :type task_list: string array
        :param task_list: the failed task_id list, e.g ['failed_task_id_1', 'failed_task_id_2',...], currently the max retry task count 10 every time

        :return: RetryShipperTasksResponse

        :raise: LogException
        """
        headers = {}
        params = {}
        body = six.b(json.dumps(task_list))
        headers['Content-Type'] = 'application/json'
        headers['x-log-bodyrawsize'] = str(len(body))
        resource = "/logstores/" + logstore_name + "/shipper/" + shipper_name + "/tasks"

        (resp, header) = self._send("PUT", project_name, body, resource, params, headers)
        return RetryShipperTasksResponse(header, resp)

    def check_upsert_scheduled_sql(self, project_name, scheduled_sql, method, resource):
        config = scheduled_sql.getConfiguration()
        if not isinstance(config, ScheduledSQLConfiguration):
            raise LogException('BadConfig', 'the scheduled sql config is not ScheduledSQLConfiguration type !')

        from_time = config.getFromTime()
        to_time = config.getToTime()

        time_range = 1451577600 < from_time < to_time
        sustained = from_time > 1451577600 and to_time == 0
        if not time_range and not sustained:
            raise LogException('BadParameters', 'Invalid fromTime: {} toTime: {}, please ensure fromTime more than '
                                                '1451577600.'.format(from_time, to_time))

        params = {}
        body = six.b(json.dumps(scheduled_sql.scheduled_sql_to_dict()))
        headers = {"x-log-bodyrawsize": str(len(body)), "Content-Type": "application/json"}
        (resp, header) = self._send(method, project_name, body, resource, params, headers)
        return resp, header

    def create_scheduled_sql(self, project_name, scheduled_sql):
        """ Create an scheduled sql job
        Unsuccessful operation will cause an LogException.
        :type project_name: string
        :param project_name: the Project name
        :type scheduled_sql: ScheduledSQL
        :param scheduled_sql: the scheduled sql job configuration
        :return:
        """
        resource = "/jobs"
        resp, header = self.check_upsert_scheduled_sql(project_name, scheduled_sql, "POST", resource)
        return CreateScheduledSQLResponse(header, resp)

    def update_scheduled_sql(self, project_name, scheduled_sql):
        """ Update an existing scheduled sql job
          Unsuccessful operation will cause an LogException.
          :type project_name: string
          :param project_name: the Project name
          :type scheduled_sql: ScheduledSQL
          :param scheduled_sql: the scheduled sql job configuration
          :return:
          """
        name = scheduled_sql.getName()
        resource = "/jobs/" + name
        resp, header = self.check_upsert_scheduled_sql(project_name, scheduled_sql, "PUT", resource)
        return UpdateScheduledSQLResponse(header, resp)

    def delete_scheduled_sql(self, project_name, job_name):
        """ Delete an existing scheduled sql job
         Unsuccessful operation will cause an LogException.
         :type project_name: string
         :param project_name: the Project name
         :type job_name: string
         :param job_name: the name of the scheduled sql job to delete
         :return:
         """
        resource = "/jobs/" + job_name
        params = {}
        headers = {}
        (resp, header) = self._send("DELETE", project_name, None, resource, params, headers)
        return DeleteScheduledSQLResponse(header, resp)

    def get_scheduled_sql(self, project_name, job_name):
        """ Get an existing scheduled sql job
          Unsuccessful operation will cause an LogException.
          :type project_name: string
          :param project_name: the Project name
          :type job_name: string
          :param job_name: the name of the scheduled sql job to get
          :return:
          """
        resource = "/jobs/" + job_name
        params = {}
        headers = {}
        (resp, header) = self._send("GET", project_name, None, resource, params, headers)
        return GetScheduledSQLResponse(header, resp)

    def list_scheduled_sql(self, project_name, offset=0, size=100):
        """ List all scheduled sql jobs
           Unsuccessful operation will cause an LogException.
           :type project_name: string
           :param project_name: the Project name
           :type offset: int
           :param offset: the offset of the list
           :type size: int
           :param size: the size of the list
           :return:
           """
        if int(size) == -1 or int(size) > MAX_LIST_PAGING_SIZE:
            return list_more(self.list_scheduled_sql, int(offset), int(size), MAX_LIST_PAGING_SIZE,
                             project_name)

        resource = '/jobs'
        headers = {}
        params = {'offset': str(offset), 'size': str(size), 'jobType': "ScheduledSQL"}
        (resp, header) = self._send("GET", project_name, None, resource, params, headers)
        return ListScheduledSQLResponse(header, resp)

    def list_scheduled_sql_job_instance(self, project, job_name, from_time, to_time, state=None, offset=0, size=100):
        """
        List scheduledSql instances.

        Unsuccessful operation will cause a LogException.

        :type project: string
        :param project: the Project name
        :type job_name: string
        :param job_name: the scheduledSql name
        :type state: string
        :param state: instance state: SUCCEEDED, FAILED, RUNNING
        :type from_time: int
        :param from_time: the begin timestamp or time. Left closed right open, must be hour time
        :type to_time: int
        :param to_time: the end timestamp or time. Left closed right open, must be hour time
        :type offset: int
        :param offset: line offset of return logs
        :type size: int
        :param size: max line number of return logs, -1 means get all
        :return: ListScheduledSqlJobInstanceResponse
        :raise: LogException
        """
        if int(size) == -1 or int(size) > MAX_LIST_PAGING_SIZE:
            return list_more(self.list_scheduled_sql_job_instance, int(offset), int(size), MAX_LIST_PAGING_SIZE,
                             project)

        headers = {}
        params = {
            'offset': str(offset),
            'size': str(size),
            'jobType': 'ScheduledSQL',
            'start': parse_timestamp(from_time),
            'end': parse_timestamp(to_time)
        }
        if state:
            params['state'] = state
        resource = '/jobs/' + job_name + '/jobinstances'
        (resp, header) = self._send('GET', project, None, resource, params, headers)
        return ListScheduledSqlJobInstancesResponse(header, resp)

    def get_scheduled_sql_job_instance(self, project, job_name, instance_id, result=None):
        """ get scheduledSqlInstance
        Unsuccessful operation will cause an LogException.
        :type project: string
        :param project: the Project name
        :type job_name: string
        :param job_name: the scheduledSql name
        :type instance_id: string
        :param instance_id: the scheduledSqlInstance id
        :type result: string
        :param result: is need details   ex:true
        :return: GetScheduledSqlJobResponse
        :raise: LogException
        """
        resource = "/jobs/{}/jobinstances/{}".format(job_name, instance_id)
        params = {}
        if result:
            params['result'] = result
        headers = {}
        (resp, header) = self._send("GET", project, None, resource, params, headers)
        return GetScheduledSqlJobInstanceResponse(header, resp)

    def modify_scheduled_sql_job_instance_state(self, project, job_name, instance_id, state):
        """ get scheduledSqlInstance
        Unsuccessful opertaion will cause an LogException.
        :type project: string
        :param project: the Project name
        :type job_name: string
        :param job_name: the scheduledSql name
        :type instance_id: string
        :param instance_id: the scheduledSqlInstance id
        :type state: string
        :param state: Modify instance state   state:FAILED、SUCCEEDED、RUNNING,only support to RUNNING
        :return: ModifyScheduledSqlJobStateResponse
        :raise: LogException
        """
        if state != "RUNNING":
            raise LogException('BadParameters',
                               "Invalid state: {}, state must be RUNNING.".format(state))
        resource = "/jobs/{}/jobinstances/{}".format(job_name, instance_id)
        params = {'state': state}
        headers = {}
        (resp, header) = self._send("PUT", project, None, resource, params, headers)
        return ModifyScheduledSqlJobStateResponse(header, resp)

    def create_project(self, project_name, project_des, resource_group_id=''):
        """ Create a project
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type project_des: string
        :param project_des: the description of a project

        type resource_group_id: string
        :param resource_group_id: the resource group id, the project created will put in the resource group

        :return: CreateProjectResponse

        :raise: LogException
        """

        params = {}
        body = {"projectName": project_name, "description": project_des, "resourceGroupId": resource_group_id}

        body = six.b(json.dumps(body))
        headers = {'Content-Type': 'application/json', 'x-log-bodyrawsize': str(len(body))}
        resource = "/"

        (resp, header) = self._send("POST", project_name, body, resource, params, headers)
        return CreateProjectResponse(header, resp)

    def get_project(self, project_name):
        """ get project
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :return: GetProjectResponse

        :raise: LogException
        """
        headers = {}
        params = {}
        resource = "/"

        (resp, header) = self._send("GET", project_name, None, resource, params, headers)
        return GetProjectResponse(resp, header)

    def delete_project(self, project_name):
        """ delete project
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :return: DeleteProjectResponse

        :raise: LogException
        """
        headers = {}
        params = {}
        resource = "/"

        (resp, header) = self._send("DELETE", project_name, None, resource, params, headers)
        return DeleteProjectResponse(header, resp)

    def change_resource_group(self, resource_id, resource_group_id, resource_type="PROJECT"):
        """
        Update the resource group of project

        :type resource_id: string
        :param resource_id: resource id

        :type resource_group_id: string
        :param resource_group_id: the resource group

        :type resource_type: string
        :param resource_type: the resource type (now only support PROJECT)

        :return: ChangeResourceGroupResponse

        :raise: LogException
        """
        params = {}
        body = {'resourceId':resource_id, 'resourceGroupId':resource_group_id, 'resourceType':resource_type}
        body_str = six.b(json.dumps(body))
        headers = {'Content-Type': 'application/json', 'x-log-bodyrawsize': str(len(body_str))}
        resource = "/resourcegroup"
        (resp, header) = self._send("PUT", resource_id, body_str, resource, params, headers)
        return LogResponse(header, resp)

    def tag_project(self, project_name, **tags):
        """ tag project
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :return: LogResponse

        :raise: LogException
        """
        resource = "/tag"
        body = {
            "resourceType": "project",
            "resourceId": [project_name],
            "tags": [{"key": str(k), "value": str(v)} for k, v in tags.items()],
        }
        body = json.dumps(body).encode()
        resp, header = self._send("POST", None, body, resource, {}, {})
        return LogResponse(header, resp)

    def untag_project(self, project_name, *tag_keys):
        """ untag project
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :return: LogResponse

        :raise: LogException
        """
        resource = "/untag"
        body = {
            "resourceType": "project",
            "resourceId": [project_name],
            "tags": tag_keys,
        }
        body = json.dumps(body).encode()
        resp, header = self._send("POST", None, body, resource, {}, {})
        return LogResponse(header, resp)

    def get_project_tags(self, project_name, **filer_tags):
        """ get project tags
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :return: GetProjectTagsResponse

        :raise: LogException
        """
        resource = "/tags"
        filer_tags = [{"key": str(k), "value": str(v)} for k, v in filer_tags.items()]
        params = {
            "resourceType": "project",
            "resourceId": json.dumps([project_name]),
            "tags": json.dumps(filer_tags),
            "nextToken": "",
        }
        while True:
            resp, header = self._send("GET", None, None, resource, params, {})
            resp = GetProjectTagsResponse(header, resp)
            yield resp

            if resp.next_token == "":
                break
            params["nextToken"] = resp.next_token

    def tag_resources(self, resource_type, resource_id, **tags):
        """ tag resources
        Unsuccessful operation will cause an LogException.

        :type resource_type: string
        :param resource_type: the resource type, currently only support project, logstore, dashboard, machine_group, logtail_config

        :type resource_id: string
        :param resource_id: the resource id, if resource_type equals project resource_id equals project_name, else resource_id equals project_name + # + subResourceId

        :return: LogResponse

        :raise: LogException
        """
        project_name = None
        if resource_type.lower() == "project":
            project_name = resource_id
        else:
            position = resource_id.find("#")
            if position != -1:
                project_name = resource_id[:position]
        resource = "/tag"
        body = {
            'resourceType': resource_type,
            'resourceId': [resource_id],
            'tags': [{'key': str(k), 'value': str(v)} for k, v in tags.items()],
        }
        body = json.dumps(body).encode()
        resp, header = self._send("POST", project_name, body, resource, {}, {})
        return LogResponse(header, resp)

    def untag_resources(self, resource_type, resource_id, *tag_keys):
        """ untag resources
        Unsuccessful operation will cause an LogException.

        :type resource_type: string
        :param resource_type: the resource type, currently only support project, logstore, dashboard, machine_group, logtail_config

        :type resource_id: string
        :param resource_id: the resource id, if resource_type equals project resource_id equals project_name, else resource_id equals project_name + # + subResourceId

        :return: LogResponse

        :raise: LogException
        """
        project_name = None
        if resource_type.lower() == "project":
            project_name = resource_id
        else:
            position = resource_id.find("#")
            if position != -1:
                project_name = resource_id[:position]
        resource = "/untag"
        body = {
            'resourceType': resource_type,
            'resourceId': [resource_id],
            'tags': tag_keys,
        }
        body = json.dumps(body).encode()
        resp, header = self._send("POST", project_name, body, resource, {}, {})
        return LogResponse(header, resp)

    def list_tag_resources(self, resource_type, resource_id, **filer_tags):
        """ list resource tags
        Unsuccessful operation will cause an LogException.

        :type resource_type: string
        :param resource_type: the resource type, currently only support project, logstore, dashboard, machine_group, logtail_config

        :type resource_id: string
        :param resource_id: the resource id, if resource_type equals project resource_id equals project_name, else resource_id equals project_name + # + subResourceId

        :return: LogResponse

        :raise: LogException
        """
        resource = "/tags"
        filer_tags = [{"key": str(k), "value": str(v)} for k, v in filer_tags.items()]
        params = {
            'resourceType': resource_type,
            'tags': json.dumps(filer_tags),
            'nextToken': "",
        }
        project_name = None
        if resource_id is not None and resource_id != "":
            params['resourceId'] = json.dumps([resource_id])
            if resource_type.lower() == "project":
                project_name = resource_id
            else:
                position = resource_id.find("#")
                if position != -1:
                    project_name = resource_id[:position]
        while True:
            resp, header = self._send("GET", project_name, None, resource, params, {})
            resp = GetResourceTagsResponse(header, resp)
            yield resp

            if resp.next_token == "":
                break
            params["nextToken"] = resp.next_token


    def create_consumer_group(self, project, logstore, consumer_group, timeout, in_order=False):
        """ create consumer group

        :type project: string
        :param project: project name

        :type logstore: string
        :param logstore: logstore name

        :type consumer_group: string
        :param consumer_group: consumer group name

        :type timeout: int
        :param timeout: time-out

        :type in_order: bool
        :param in_order: if consume in order, default is False

        :return: CreateConsumerGroupResponse
        """
        request = CreateConsumerGroupRequest(project, logstore, ConsumerGroupEntity(consumer_group, timeout, in_order))
        consumer_group = request.consumer_group
        body_str = consumer_group.to_request_json()

        headers = {
            "x-log-bodyrawsize": '0',
            "Content-Type": "application/json"
        }
        params = {}

        project = request.get_project()
        resource = "/logstores/" + request.get_logstore() + "/consumergroups"
        (resp, header) = self._send("POST", project, body_str, resource, params, headers)
        return CreateConsumerGroupResponse(header, resp)

    def update_consumer_group(self, project, logstore, consumer_group, timeout=None, in_order=None):
        """ Update consumer group

        :type project: string
        :param project: project name

        :type logstore: string
        :param logstore: logstore name

        :type consumer_group: string
        :param consumer_group: consumer group name

        :type timeout: int
        :param timeout: timeout

        :type in_order: bool
        :param in_order: order

        :return: None
        """
        if in_order is None and timeout is None:
            raise ValueError('in_order and timeout can\'t all be None')
        elif in_order is not None and timeout is not None:
            body_dict = {
                'order': in_order,
                'timeout': timeout
            }
        elif in_order is not None:
            body_dict = {
                'order': in_order
            }
        else:
            body_dict = {
                'timeout': timeout
            }
        body_str = six.b(json.dumps(body_dict))

        headers = {
            "x-log-bodyrawsize": str(len(body_str)),
            "Content-Type": "application/json"
        }
        params = {}
        resource = "/logstores/" + logstore + "/consumergroups/" + consumer_group
        (resp, header) = self._send("PUT", project, body_str, resource, params, headers)
        return UpdateConsumerGroupResponse(header, resp)

    def delete_consumer_group(self, project, logstore, consumer_group):
        """ Delete consumer group

        :type project: string
        :param project: project name

        :type logstore: string
        :param logstore: logstore name

        :type consumer_group: string
        :param consumer_group: consumer group name

        :return: None
        """

        headers = {"x-log-bodyrawsize": '0'}
        params = {}

        resource = "/logstores/" + logstore + "/consumergroups/" + consumer_group
        (resp, header) = self._send("DELETE", project, None, resource, params, headers)
        return DeleteConsumerGroupResponse(header, resp)

    def list_consumer_group(self, project, logstore):
        """ List consumer group

        :type project: string
        :param project: project name

        :type logstore: string
        :param logstore: logstore name

        :return: ListConsumerGroupResponse
        """

        resource = "/logstores/" + logstore + "/consumergroups"
        params = {}
        headers = {}

        (resp, header) = self._send("GET", project, None, resource, params, headers)
        return ListConsumerGroupResponse(resp, header)

    def update_check_point(self, project, logstore, consumer_group, shard, check_point,
                           consumer='', force_success=True):
        """ Update check point

        :type project: string
        :param project: project name

        :type logstore: string
        :param logstore: logstore name

        :type consumer_group: string
        :param consumer_group: consumer group name

        :type shard: int
        :param shard: shard id

        :type check_point: string
        :param check_point: checkpoint name

        :type consumer: string
        :param consumer: consumer name

        :type force_success: bool
        :param force_success: if force to succeed

        :return: None
        """
        request = ConsumerGroupUpdateCheckPointRequest(project, logstore, consumer_group,
                                                       consumer, shard, check_point, force_success)
        params = request.get_request_params()
        body_str = request.get_request_body()
        headers = {"Content-Type": "application/json"}
        resource = "/logstores/" + logstore + "/consumergroups/" + consumer_group
        (resp, header) = self._send("POST", project, body_str, resource, params, headers)
        return ConsumerGroupUpdateCheckPointResponse(header, resp)

    def get_check_point(self, project, logstore, consumer_group, shard=-1):
        """ Get check point

        :type project: string
        :param project: project name

        :type logstore: string
        :param logstore: logstore name

        :type consumer_group: string
        :param consumer_group: consumer group name

        :type shard: int
        :param shard: shard id

        :return: ConsumerGroupCheckPointResponse
        """
        request = ConsumerGroupGetCheckPointRequest(project, logstore, consumer_group, shard)
        params = request.get_params()
        headers = {}
        resource = "/logstores/" + logstore + "/consumergroups/" + consumer_group
        (resp, header) = self._send("GET", project, None, resource, params, headers)
        return ConsumerGroupCheckPointResponse(resp, header)

    def get_check_point_fixed(self, project, logstore, consumer_group, shard=-1):
        """ Get check point

        :type project: string
        :param project: project name

        :type logstore: string
        :param logstore: logstore name

        :type consumer_group: string
        :param consumer_group: consumer group name

        :type shard: int
        :param shard: shard id

        :return: ConsumerGroupCheckPointResponse
        """

        res = self.get_check_point(project, logstore, consumer_group, shard)
        res.check_checkpoint(self, project, logstore)

        return res

    def heart_beat(self, project, logstore, consumer_group, consumer, shards=None):
        """ Heatbeat consumer group

        :type project: string
        :param project: project name

        :type logstore: string
        :param logstore: logstore name

        :type consumer_group: string
        :param consumer_group: consumer group name

        :type consumer: string
        :param consumer: consumer name

        :type shards: int list
        :param shards: shard id list e.g. [0,1,2]

        :return: None
        """
        if shards is None:
            shards = []
        request = ConsumerGroupHeartBeatRequest(project, logstore, consumer_group, consumer, shards)
        body_str = request.get_request_body()
        params = request.get_params()
        headers = {"Content-Type": "application/json"}
        resource = "/logstores/" + logstore + "/consumergroups/" + consumer_group
        (resp, header) = self._send('POST', project, body_str, resource, params, headers)
        return ConsumerGroupHeartBeatResponse(resp, header)

    def copy_project(self, from_project, to_project, to_client=None, copy_machine_group=False):
        """
        copy project, logstore, machine group and logtail config to target project,
        expecting the target project doesn't contain same named logstores as source project

        :type from_project: string
        :param from_project: project name

        :type to_project: string
        :param to_project: project name

        :type to_client: LogClient
        :param to_client: logclient instance

        :type copy_machine_group: bool
        :param copy_machine_group: if copy machine group resources, False by default.

        :return: None
        """
        if to_client is None:
            to_client = self
        return copy_project(self, to_client, from_project, to_project, copy_machine_group)

    def copy_logstore(self, from_project, from_logstore, to_logstore, to_project=None, to_client=None, to_region_endpoint=None):
        """
        copy logstore, index, logtail config to target logstore, machine group are not included yet.
        the target logstore will be crated if not existing

        :type from_project: string
        :param from_project: project name

        :type from_logstore: string
        :param from_logstore: logstore name

        :type to_logstore: string
        :param to_logstore: target logstore name

        :type to_project: string
        :param to_project: target project name, copy to same project if not being specified, will try to create it if not being specified

        :type to_client: LogClient
        :param to_client: logclient instance, use it to operate on the "to_project" if being specified for cross region purpose

        :type to_region_endpoint: string
        :param to_region_endpoint: target region, use it to operate on the "to_project" while "to_client" not be specified

        :return:
        """
        return copy_logstore(self, from_project, from_logstore, to_logstore, to_project=to_project, to_client=to_client, to_region_endpoint=to_region_endpoint)

    def list_project(self, offset=0, size=100, project_name_pattern=None, resource_group_id=''):
        """ list the project
        Unsuccessful operation will cause an LogException.

        :type project_name_pattern: string
        :param project_name_pattern: the sub name project, used for the server to return project names contain this sub name

        :type offset: int
        :param offset: the offset of all the matched names

        :type size: int
        :param size: the max return names count, -1 means return all data

        :type resource_group_id: string
        :param resource_group_id: the resource group id, used for the server to return project in resource group

        :return: ListProjectResponse

        :raise: LogException
        """

        # need to use extended method to get more
        if int(size) == -1 or int(size) > MAX_LIST_PAGING_SIZE:
            return list_more(self.list_project, int(offset), int(size), MAX_LIST_PAGING_SIZE)

        headers = {}
        params = {}
        resource = "/"
        if project_name_pattern is not None:
            params['projectName'] = project_name_pattern
        params['offset'] = str(offset)
        params['size'] = str(size)
        params['resourceGroupId'] = resource_group_id
        (resp, header) = self._send("GET", None, None, resource, params, headers)
        return ListProjectResponse(resp, header)

    def es_migration(
            self,
            cache_path,
            hosts,
            project_name,
            indexes=None,
            query=None,
            logstore_index_mappings=None,
            pool_size=None,
            time_reference=None,
            source=None,
            topic=None,
            batch_size=None,
            wait_time_in_secs=None,
            auto_creation=True,
            retries_failed=None,
            cache_duration="1d",
    ):
        """
        Migrate data from elasticsearch to aliyun log service (SLS)

        :type cache_path: string
        :param cache_path: file path to store migration cache, which used for resuming migration process when stopped. Please ensure it's clean for new migration task.

        :type hosts: string
        :param hosts: a comma-separated list of source ES nodes. e.g. "localhost:9200,other_host:9200"

        :type project_name: string
        :param project_name: specify the project_name of your log services. e.g. "your_project"

        :type indexes: string
        :param indexes: a comma-separated list of source index names. e.g. "index1,index2"

        :type query: string
        :param query: used to filter docs, so that you can specify the docs you want to migrate. e.g. '{"query": {"match": {"title": "python"}}}'

        :type logstore_index_mappings: string
        :param logstore_index_mappings: specify the mappings of log service logstore and ES index. e.g. '{"logstore1": "my_index*", "logstore2": "index1,index2"}}'

        :type pool_size: int
        :param pool_size: specify the size of migration task process pool. Default is 10 if not set.

        :type time_reference: string
        :param time_reference: specify what ES doc's field to use as log's time field. e.g. "field1"

        :type source: string
        :param source: specify the value of log's source field. e.g. "your_source"

        :type topic: string
        :param topic: specify the value of log's topic field. e.g. "your_topic"

        :type batch_size: int
        :param batch_size: max number of logs written into SLS in a batch. SLS requires that it's no bigger than 512KB in size and 1024 lines in one batch. Default is 1000 if not set.

        :type wait_time_in_secs: int
        :param wait_time_in_secs: specify the waiting time between initialize aliyun log and executing data migration task. Default is 60 if not set.

        :type auto_creation: bool
        :param auto_creation: specify whether to let the tool create logstore and index automatically for you. e.g. True

        :type retries_failed: int
        :param retries_failed: specify retrying times for failed tasks. e.g. 10

        :type cache_duration: str
        :param cache_duration: the max duration of non-updating cache, which is based on scroll of elasticsearch. Max duration is "1d". Default is "1d"

        :return: LogResponse

        :raise: Exception
        """
        from .es_migration import MigrationManager, MigrationConfig

        config = MigrationConfig(
            cache_path=cache_path,
            hosts=hosts,
            indexes=indexes,
            query=query,
            scroll=cache_duration,
            project_name=project_name,
            logstore_index_mappings=logstore_index_mappings,
            pool_size=pool_size,
            time_reference=time_reference,
            source=source,
            topic=topic,
            batch_size=batch_size,
            wait_time_in_secs=wait_time_in_secs,
            auto_creation=auto_creation,
            retries_failed=retries_failed,
            endpoint=self._endpoint,
            access_key_id=self._accessKeyId,
            access_key=self._accessKey,
        )
        migration_manager = MigrationManager(config)
        migration_manager.migrate()
        return LogResponse({})

    def copy_data(self, project, logstore, from_time, to_time=None,
                  to_client=None, to_project=None, to_logstore=None,
                  shard_list=None,
                  batch_size=None, compress=None, new_topic=None, new_source=None):
        """
        copy data from one logstore to another one (could be the same or in different region), the time is log received time on server side.

        :type project: string
        :param project: project name

        :type logstore: string
        :param logstore: logstore name

        :type from_time: string/int
        :param from_time: curosr value, could be begin, timestamp or readable time in readable time like "%Y-%m-%d %H:%M:%S<time_zone>" e.g. "2018-01-02 12:12:10+8:00", also support human readable string, e.g. "1 hour ago", "now", "yesterday 0:0:0", refer to https://aliyun-log-cli.readthedocs.io/en/latest/tutorials/tutorial_human_readable_datetime.html

        :type to_time: string/int
        :param to_time: curosr value, default is "end", could be begin, timestamp or readable time in readable time like "%Y-%m-%d %H:%M:%S<time_zone>" e.g. "2018-01-02 12:12:10+8:00", also support human readable string, e.g. "1 hour ago", "now", "yesterday 0:0:0", refer to https://aliyun-log-cli.readthedocs.io/en/latest/tutorials/tutorial_human_readable_datetime.html

        :type to_client: LogClient
        :param to_client: logclient instance, if empty will use source client

        :type to_project: string
        :param to_project: project name, if empty will use source project

        :type to_logstore: string
        :param to_logstore: logstore name, if empty will use source logstore

        :type shard_list: string
        :param shard_list: shard number list. could be comma seperated list or range: 1,20,31-40

        :type batch_size: int
        :param batch_size: batch size to fetch the data in each iteration. by default it's 500

        :type compress: bool
        :param compress: if use compression, by default it's True

        :type new_topic: string
        :param new_topic: overwrite the copied topic with the passed one

        :type new_source: string
        :param new_source: overwrite the copied source with the passed one

        :return: LogResponse {"total_count": 30, "shards": {0: 10, 1: 20} })

        """
        return copy_data(self, project, logstore, from_time, to_time=to_time,
                         to_client=to_client, to_project=to_project, to_logstore=to_logstore,
                         shard_list=shard_list,
                         batch_size=batch_size, compress=compress, new_topic=new_topic, new_source=new_source)

    def transform_data(self, project, logstore, config, from_time, to_time=None,
                       to_client=None, to_project=None, to_logstore=None,
                       shard_list=None,
                       batch_size=None, compress=None,
                       cg_name=None, c_name=None,
                       cg_heartbeat_interval=None, cg_data_fetch_interval=None, cg_in_order=None,
                       cg_worker_pool_size=None
                       ):
        """
        transform data from one logstore to another one (could be the same or in different region), the time passed is log received time on server side. There're two mode, batch mode / consumer group mode. For Batch mode, just leave the cg_name and later options as None.

        :type project: string
        :param project: project name

        :type logstore: string
        :param logstore: logstore name

        :type config: string
        :param config: transform config imported or path of config (in python)

        :type from_time: string/int
        :param from_time: curosr value, could be begin, timestamp or readable time in readable time like "%Y-%m-%d %H:%M:%S<time_zone>" e.g. "2018-01-02 12:12:10+8:00", also support human readable string, e.g. "1 hour ago", "now", "yesterday 0:0:0", refer to https://aliyun-log-cli.readthedocs.io/en/latest/tutorials/tutorial_human_readable_datetime.html

        :type to_time: string/int
        :param to_time: curosr value, could be begin, timestamp or readable time in readable time like "%Y-%m-%d %H:%M:%S<time_zone>" e.g. "2018-01-02 12:12:10+8:00", also support human readable string, e.g. "1 hour ago", "now", "yesterday 0:0:0", refer to https://aliyun-log-cli.readthedocs.io/en/latest/tutorials/tutorial_human_readable_datetime.html

        :type to_client: LogClient
        :param to_client: logclient instance, if empty will use source client

        :type to_project: string
        :param to_project: project name, if empty will use source project

        :type to_logstore: string
        :param to_logstore: logstore name, if empty will use source logstore

        :type shard_list: string
        :param shard_list: shard number list. could be comma seperated list or range: 1,20,31-40

        :type batch_size: int
        :param batch_size: batch size to fetch the data in each iteration. by default it's 500

        :type compress: bool
        :param compress: if use compression, by default it's True

        :type cg_name: string
        :param cg_name: consumer group name to enable scability and availability support.

        :type c_name: string
        :param c_name: consumer name for consumer group mode, must be different for each consuer in one group, normally leave it as default:  CLI-transform-data-${process_id}

        :type cg_heartbeat_interval: int
        :param cg_heartbeat_interval: cg_heartbeat_interval, default 20

        :type cg_data_fetch_interval: int
        :param cg_data_fetch_interval: cg_data_fetch_interval, default 2

        :type cg_in_order: bool
        :param cg_in_order: cg_in_order, default False

        :type cg_worker_pool_size: int
        :param cg_worker_pool_size: cg_worker_pool_size, default 2

        :return: LogResponse {"total_count": 30, "shards": {0: {"count": 10, "removed": 1},  2: {"count": 20, "removed": 1}} })

        """
        return transform_data(self, project, logstore, from_time, to_time=to_time,
                              config=config,
                              to_client=to_client, to_project=to_project, to_logstore=to_logstore,
                              shard_list=shard_list,
                              batch_size=batch_size, compress=compress,
                              cg_name=cg_name, c_name=c_name,
                              cg_heartbeat_interval=cg_heartbeat_interval,
                              cg_data_fetch_interval=cg_data_fetch_interval,
                              cg_in_order=cg_in_order,
                              cg_worker_pool_size=cg_worker_pool_size
                              )

    def get_resource_usage(self, project):
        """ get resource usage ist the project
        Unsuccessful operation will cause an LogException.

        :type client: string
        :param client: project name

        :return: dict

        :raise: LogException
        """
        return get_resource_usage(self, project)

    def arrange_shard(self, project, logstore, count):
        """ arrange shard to the expected read-write count to a larger one.

        :type project: string
        :param project: project name

        :type logstore: string
        :param logstore: logstore name

        :type count: int
        :param count: expected read-write shard count. should be larger than the current one.

        :return: ''

        :raise: LogException
        """
        return arrange_shard(self, project, logstore, count)

    def enable_alert(self, project_name, job_name):
        """ enable apert
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type job_name: string
        :param job_name: the Alert Job name

        :return: LogResponse

        :raise: LogException
        """
        headers = {}
        params = {"action": "enable"}
        resource = "/jobs/" + job_name
        (resp, header) = self._send("PUT", project_name, None, resource, params, headers)
        return LogResponse(header)

    def disable_alert(self, project_name, job_name):
        """ disable apert
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type job_name: string
        :param job_name: the Alert Job name

        :return: LogResponse

        :raise: LogException
        """
        headers = {}
        params = {"action": "disable"}
        resource = "/jobs/" + job_name
        (resp, header) = self._send("PUT", project_name, None, resource, params, headers)
        return LogResponse(header)

    def list_ingestion(self, project_name, logstore_name='', offset=0, size=100):
        """ list ingestion
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :return: ListIngestionResponse

        :raise: LogException
        """

        headers = {}
        params = {}
        params['jobType'] = 'Ingestion'
        params['logstore'] = logstore_name
        params['offset'] = str(offset)
        params['size'] = str(size)
        resource = '/jobs'
        (resp, header) = self._send("GET", project_name, None, resource, params, headers)
        return ListIngestionResponse(resp, header)

    def create_ingestion(self, project_name, ingestion_config):
        """ create ingestion config
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type ingestion_config: string
        :param ingestion_config: the ingestion config

        :return: CreateIngestionResponse

        :raise: LogException
        """

        headers = {}
        params = {}
        resource = "/jobs"
        headers['Content-Type'] = 'application/json'
        body = six.b(ingestion_config)
        headers['x-log-bodyrawsize'] = str(len(body))

        (resp, header) = self._send("POST", project_name, body, resource, params, headers)
        return CreateIngestionResponse(header, resp)

    def update_ingestion(self, project_name, ingestion_name, ingestion_config):
        """ update ingestion config
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type ingestion_name: string
        :param ingestion_name: the ingestion name

        :type ingestion_config: string
        :param ingestion_config: the ingestion config

        :return: UpdateIngestionResponse

        :raise: LogException
        """

        headers = {}
        params = {}
        resource = "/jobs/" + ingestion_name
        headers['Content-Type'] = 'application/json'
        body = six.b(ingestion_config)
        headers['x-log-bodyrawsize'] = str(len(body))

        (resp, header) = self._send("PUT", project_name, body, resource, params, headers)
        return UpdateIngestionResponse(header, resp)

    def delete_ingestion(self, project_name, ingestion_name):
        """ delete ingestion config
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type ingestion_name: string
        :param ingestion_name: the ingestion name

        :return: DeleteIngestionResponse

        :raise: LogException
        """

        headers = {}
        params = {}
        resource = "/jobs/" + ingestion_name
        (resp, header) = self._send("DELETE", project_name, None, resource, params, headers)
        return DeleteIngestionResponse(header, resp)

    def get_ingestion(self, project_name, ingestion_name):
        """ get ingestion config detail
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type ingestion_name: string
        :param ingestion_name: the ingestion name

        :return: GetIngestionResponse

        :raise: LogException
        """

        headers = {}
        params = {}
        resource = "/jobs/" + ingestion_name
        (resp, header) = self._send("GET", project_name, None, resource, params, headers)
        return GetIngestionResponse(resp, header)

    def start_ingestion(self, project_name, ingestion_name):
        """ start ingestion
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type ingestion_name: string
        :param ingestion_name: the ingestion name

        :return: StartIngestionResponse

        :raise: LogException
        """

        headers = {}
        params = {"action":"START"}
        resource = "/jobs/" + ingestion_name

        (resp, header) = self._send("PUT", project_name, None, resource, params, headers)
        return StartIngestionResponse(header, resp)

    def restart_ingestion(self, project_name, ingestion_name, ingestion_config):
        """ restart ingestion
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type ingestion_name: string
        :param ingestion_name: the ingestion name

        :return: RestartIngestionResponse

        :raise: LogException
        """

        headers = {}
        params = {"action":"RESTART"}
        resource = "/jobs/" + ingestion_name
        headers['Content-Type'] = 'application/json'
        body = six.b(ingestion_config)
        headers['x-log-bodyrawsize'] = str(len(body))

        (resp, header) = self._send("PUT", project_name, body, resource, params, headers)
        return RestartIngestionResponse(header, resp)

    def stop_ingestion(self, project_name, ingestion_name):
        """ stop ingestion
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type ingestion_name: string
        :param ingestion_name: the ingestion name

        :return: StopIngestionResponse

        :raise: LogException
        """

        headers = {}
        params = {"action":"STOP"}
        resource = "/jobs/" + ingestion_name

        (resp, header) = self._send("PUT", project_name, None, resource, params, headers)
        return StopIngestionResponse(header, resp)

    def create_etl(self, project_name, name, configuration, schedule, display_name, description=None):
        """ create etl
        Unsuccessful operation will cause an LogException.
        :type project_name: string
        :param project_name: the Project name
        :type name: string
        :param name: the etl name
        :type configuration: dict
        :param configuration: the ETLConfiguration
        :type schedule: dict
        :param schedule: the JobSchedule
        :type display_name: string
        :param display_name: the etl display name
        :type description: string
        :param description: the etl description
        :return: CreateEtlResponse
        :raise: LogException
        """
        params = {}
        resource = "/jobs"
        body = {
            'name': name,
            'configuration': configuration,
            'schedule': schedule,
            'description': description,
            'displayName': display_name,
            'type': 'ETL'
        }
        body_str = six.b(json.dumps(body))
        headers = {"x-log-bodyrawsize": str(len(body_str)), "Content-Type": "application/json"}
        (resp, header) = self._send("POST", project_name, body_str, resource, params, headers)
        return CreateEtlResponse(header, resp)

    def get_etl(self, project_name, name):
        """ get etl
        Unsuccessful operation will cause an LogException.
        :type project_name: string
        :param project_name: the Project name
        :type name: string
        :param name: the etl name
        :return: GetEtlResponse
        :raise: LogException
        """
        headers = {}
        params = {}
        resource = "/jobs/" + name
        (resp, header) = self._send("GET", project_name, None, resource, params, headers)
        return GetEtlResponse(header, resp)

    def update_etl(self, project_name, name, configuration, schedule, display_name, description=None):
        """ update etl
        Unsuccessful operation will cause an LogException.
        :type project_name: string
        :param project_name: the Project name
        :type name: string
        :param name: the etl name
        :type configuration: dict
        :param configuration: the ETLConfiguration
        :type schedule: dict
        :param schedule: the JobSchedule
        :type display_name: string
        :param display_name: the etl display name
        :type description: string
        :param description: the etl description
        :return: UpdateEtlResponse
        :raise: LogException
        """
        params = {}
        resource = "/jobs/" + name
        body = {
            'name': name,
            'configuration': configuration,
            'schedule': schedule,
            'description': description,
            'displayName': display_name,
            'type': 'ETL'
        }
        body_str = six.b(json.dumps(body))
        headers = {"x-log-bodyrawsize": str(len(body_str)), "Content-Type": "application/json"}
        (resp, header) = self._send("PUT", project_name, body_str, resource, params, headers)
        return UpdateEtlResponse(header, resp)

    def start_etl(self, project_name, name):
        """ start etl
        Unsuccessful operation will cause an LogException.
        :type project_name: string
        :param project_name: the Project name
        :type name: string
        :param name: the etl name
        :return: StartEtlResponse
        :raise: LogException
        """
        headers = {}
        params = {"action": "START"}
        resource = "/jobs/" + name
        (resp, header) = self._send("PUT", project_name, None, resource, params, headers)
        return StartEtlResponse(header, resp)

    def stop_etl(self, project_name, name):
        """ stop etl
        Unsuccessful operation will cause an LogException.
        :type project_name: string
        :param project_name: the Project name
        :type name: string
        :param name: the etl name
        :return: StopEtlResponse
        :raise: LogException
        """
        headers = {}
        params = {"action": "STOP"}
        resource = "/jobs/" + name
        (resp, header) = self._send("PUT", project_name, None, resource, params, headers)
        return StopEtlResponse(header, resp)

    def list_etls(self, project_name, offset=0, size=100):
        """ list etls
        Unsuccessful operation will cause an LogException.
        :type project_name: string
        :param project_name: the Project name
        :type offset: int
        :param offset: line offset of return logs
        :type size: int
        :param size: max line number of return logs, -1 means get all
        :return: ListEtlsResponse
        :raise: LogException
        """
        # need to use extended method to get more
        if int(size) == -1 or int(size) > MAX_LIST_PAGING_SIZE:
            return list_more(self.list_etls, int(offset), int(size), MAX_LIST_PAGING_SIZE,
                             project_name)
        headers = {}
        params = {}
        resource = '/jobs'
        params['offset'] = str(offset)
        params['size'] = str(size)
        params['jobType'] = "ETL"
        (resp, header) = self._send("GET", project_name, None, resource, params, headers)
        return ListEtlsResponse(resp, header)

    def delete_etl(self, project_name, name):
        """ delete etl
        Unsuccessful operation will cause an LogException.
        :type project_name: string
        :param project_name: the Project name
        :type name: string
        :param name: the etl name
        :return: DeleteEtlResponse
        :raise: LogException
        """
        headers = {}
        params = {}
        resource = "/jobs/" + name
        (resp, header) = self._send("DELETE", project_name, None, resource, params, headers)
        return DeleteEtlResponse(header, resp)

    def create_substore(self, project_name, logstore_name, substore_name, keys,
                        ttl,
                        sorted_key_count,
                        time_index
                        ):
        """ create sub store
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type logstore_name: string
        :param logstore_name: the logstore name

        :type substore_name: string
        :param substore_name: the substore name

        :type keys: list
        :param keys: the keys

        :type ttl: int
        :param ttl: the ttl

        :type sorted_key_count: int
        :param sorted_key_count: the sorted key count of the logstore to create

        :type time_Index: int
        :param time_Index: the time index of the logstore to create

        :return: CreateSubStoreResponse

        :raise: LogException
        """

        params = {}
        resource = "/logstores/" + logstore_name + "/substores"
        headers = {"x-log-bodyrawsize": '0', "Content-Type": "application/json", "Accept-Encoding": "deflate"}

        body = {"name": substore_name, "ttl": int(ttl),
                "sortedKeyCount": int(sorted_key_count),
                "timeIndex": int(time_index),
                "keys": keys
                }
        body_str = six.b(json.dumps(body))
        (resp, header) = self._send("POST", project_name, body_str, resource, params, headers)
        return CreateSubStoreResponse(header, resp)

    def delete_substore(self, project_name, logstore_name, substore_name):
        """ delete sub store
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type logstore_name: string
        :param logstore_name: the logstore name

        :type substore_name: string
        :param substore_name: the substore name

        :return: DeleteSubStoreResponse

        :raise: LogException
        """
        headers = {}
        params = {}
        resource = "/logstores/" + logstore_name + "/substores/" + substore_name
        (resp, header) = self._send("DELETE", project_name, None, resource, params, headers)
        return DeleteSubStoreResponse(header, resp)

    def get_substore(self, project_name, logstore_name, substore_name):
        """ get the substore meta info
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type logstore_name: string
        :param logstore_name: the logstore name

        :type substore_name: string
        :param substore_name: the substore name

        :return: GetSubStoreResponse

        :raise: LogException
        """

        headers = {}
        params = {}
        resource = "/logstores/" + logstore_name + "/substores/" + substore_name
        (resp, header) = self._send("GET", project_name, None, resource, params, headers)
        return GetSubStoreResponse(resp, header)

    def update_substore(self, project_name, logstore_name, substore_name, keys,
                        ttl,
                        sorted_key_count,
                        time_index
                        ):
        """ update sub store
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type logstore_name: string
        :param logstore_name: the logstore name

        :type substore_name: string
        :param substore_name: the substore name

        :type keys: list
        :param keys: the keys

        :type ttl: int
        :param ttl: the ttl

        :type sorted_key_count: int
        :param sorted_key_count: the sorted key count of the logstore to create

        :type time_Index: int
        :param time_Index: the time index of the logstore to create

        :return: UpdateSubStoreResponse

        :raise: LogException
        """

        headers = {"x-log-bodyrawsize": '0', "Content-Type": "application/json", "Accept-Encoding": "deflate"}
        params = {}
        resource = "/logstores/" + logstore_name + "/substores/" + substore_name
        body = {"name": substore_name, "ttl": int(ttl),
                "sortedKeyCount": int(sorted_key_count),
                "timeIndex": int(time_index),
                "keys": keys
                }
        body_str = six.b(json.dumps(body))
        (resp, header) = self._send("PUT", project_name, body_str, resource, params, headers)

        return UpdateSubStoreResponse(header, resp)

    def list_substore(self, project_name, logstore_name):
        """ list the substore
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type logstore_name: string
        :param logstore_name: the Logstore name

        :return: ListSubStoreResponse

        :raise: LogException
        """

        headers = {}
        params = {}
        resource = "/logstores/" + logstore_name + "/substores"
        (resp, header) = self._send("GET", project_name, None, resource, params, headers)
        return ListSubStoreResponse(resp, header)

    def get_substore_ttl(self, project_name, logstore_name):
        """ get the substore ttl
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type logstore_name: string
        :param logstore_name: the logstore name

        :return: GetSubStoreTTLResponse

        :raise: LogException
        """

        headers = {}
        params = {}
        resource = "/logstores/" + logstore_name + "/substores/storage/ttl"
        (resp, header) = self._send("GET", project_name, None, resource, params, headers)
        return GetSubStoreTTLResponse(resp, header)

    def update_substore_ttl(self, project_name, logstore_name, ttl):
        """ update the substore ttl
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type logstore_name: string
        :param logstore_name: the logstore name

        :type ttl: int
        :param ttl: the ttl

        :return: GetSubStoreTTLResponse

        :raise: LogException
        """

        headers = {"x-log-bodyrawsize": "0"}
        params = {
            "ttl": ttl,
        }
        resource = "/logstores/" + logstore_name + "/substores/storage/ttl"
        (resp, header) = self._send("PUT", project_name, None, resource, params, headers)
        return UpdateSubStoreTTLResponse(header, resp)

    def create_metric_store(self, project_name, logstore_name,
                            ttl=30,
                            shard_count=2,
                            enable_tracking=False,
                            append_meta=False,
                            auto_split=True,
                            max_split_shard=64,
                            preserve_storage=False,
                            encrypt_conf=None,
                            hot_ttl=-1,
                            mode=None
                            ):

        """ create metric store
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type logstore_name: string
        :param logstore_name: the logstore name

        :type ttl: int
        :param ttl: the life cycle of log in the logstore in days, default 30, up to 3650

        :type hot_ttl: int
        :param hot_ttl: the life cycle of hot storage,[0-hot_ttl]is hot storage, (hot_ttl-ttl] is warm storage, if hot_ttl=-1, it means [0-ttl]is all hot storage

        :type shard_count: int
        :param shard_count: the shard count of the logstore to create, default 2

        :type enable_tracking: bool
        :param enable_tracking: enable web tracking, default is False

        :type append_meta: bool
        :param append_meta: allow to append meta info (server received time and IP for external IP to each received log)

        :type auto_split: bool
        :param auto_split: auto split shard, max_split_shard will be 64 by default is True

        :type max_split_shard: int
        :param max_split_shard: max shard to split, up to 256

        :type preserve_storage: bool
        :param preserve_storage: if always persist data, TTL will be ignored.

        :type encrypt_conf: dict
        :param encrypt_conf :  following is a sample
        +       {
+                    "enable" : True/False,              # required
+                    "encrypt_type" : "default",         # required, default encrypt alogrithm only currently
+                    "user_cmk_info" :                   # optional, if 'user_cmk_info' is set, use byok cmk key, otherwise use sls system cmk key
+                    {
+                        "cmk_key_id" :                  # the cmk key used to generate data encrypt key
+                        "arn" :                         # arn to grant sls service to get/generate data encrypt key in kms
+                        "region_id" :                   # the region id of cmk_key_id
+                    }
+                }

        :type mode: string
        :param mode: type of logstore, can be choose between lite and standard, default value standard

        :return: CreateMetricsStoreResponse

        :raise: LogException
        """

        logstore_response = self.create_logstore(project_name, logstore_name, ttl=ttl,
                                                 shard_count=shard_count,
                                                 enable_tracking=enable_tracking,
                                                 append_meta=append_meta,
                                                 auto_split=auto_split,
                                                 max_split_shard=max_split_shard,
                                                 preserve_storage=preserve_storage,
                                                 encrypt_conf=encrypt_conf,
                                                 telemetry_type='Metrics',
                                                 hot_ttl=hot_ttl,
                                                 mode=mode)

        time.sleep(1)
        keys = [
            {'name': '__name__', 'type': 'text'},
            {'name': '__labels__', 'type': 'text'},
            {'name': '__time_nano__', 'type': 'long'},
            {'name': '__value__', 'type': 'double'},
        ]
        substore_response = self.create_substore(project_name, logstore_name, 'prom', ttl=ttl,
                                                 keys=keys, sorted_key_count=2, time_index=2)
        return CreateMetricsStoreResponse(logstore_response, substore_response)

    def delete_metric_store(self, project_name, logstore_name):
        """ delete metric store
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type logstore_name: string
        :param logstore_name: the logstore name

        :return: DeleteLogStoreResponse

        :raise: LogException
        """
        return self.delete_logstore(project_name, logstore_name)

    def get_metric_store(self, project_name, logstore_name):
        """ get the metric store meta info
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type logstore_name: string
        :param logstore_name: the logstore name

        :return: GetLogStoreResponse

        :raise: LogException
        """

        return self.get_logstore(project_name, logstore_name)

    def update_metric_store(self, project_name, logstore_name, ttl=None, enable_tracking=None, shard_count=None,
                            append_meta=None,
                            auto_split=None,
                            max_split_shard=None,
                            preserve_storage=None,
                            encrypt_conf=None,
                            hot_ttl=-1,
                            mode=None,
                            ):
        """
        update metric store meta info
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type logstore_name: string
        :param logstore_name: the logstore name

        :type ttl: int
        :param ttl: the life cycle of log in the logstore in days

        :type hot_ttl: int
        :param hot_ttl: the life cycle of hot storage,[0-hot_ttl]is hot storage, (hot_ttl-ttl] is warm storage, if hot_ttl=-1, it means [0-ttl]is all hot storage

        :type enable_tracking: bool
        :param enable_tracking: enable web tracking

        :type shard_count: int
        :param shard_count: deprecated, the shard count could only be updated by split & merge

        :type append_meta: bool
        :param append_meta: allow to append meta info (server received time and IP for external IP to each received log)

        :type auto_split: bool
        :param auto_split: auto split shard, max_split_shard will be 64 by default is True

        :type max_split_shard: int
        :param max_split_shard: max shard to split, up to 256

        :type preserve_storage: bool
        :param preserve_storage: if always persist data, TTL will be ignored.

        :type encrypt_conf: dict
        :param encrypt_conf :  following is a sample
+                {
+                    "enable" : True/False,              # required
+                    "encrypt_type" : "default",         # required, default encrypt alogrithm only currently
+                    "user_cmk_info" :                   # optional, if 'user_cmk_info' is set, use byok cmk key, otherwise use sls system cmk key
+                    {
+                        "cmk_key_id" :                  # the cmk key used to generate data encrypt key
+                        "arn" :                         # arn to grant sls service to get/generate data encrypt key in kms
+                        "region_id" :                   # the region id of cmk_key_id
+                    }
+                }

        :type mode: string
        :param mode: type of logstore, can be choose between lite and standard, default value standard

        :return: UpdateLogStoreResponse

        :raise: LogException
        """
        self.update_substore_ttl(project_name, logstore_name, ttl)
        return self.update_logstore(project_name, logstore_name,
                                    ttl=ttl,
                                    enable_tracking=enable_tracking,
                                    shard_count=shard_count,
                                    append_meta=append_meta,
                                    auto_split=auto_split,
                                    max_split_shard=max_split_shard,
                                    preserve_storage=preserve_storage,
                                    encrypt_conf=encrypt_conf,
                                    hot_ttl=hot_ttl,
                                    mode=mode,
                                    telemetry_type='Metrics'
                                    )

    def create_sql_instance(self, project_name, sql_instance,useAsDefault):
        """ create sql instance config
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type sql_instance: long
        :param sql_instance: the sql instance config

        :return: CreateSqlInstanceResponse

        :raise: LogException
        """
        headers = {"x-log-bodyrawsize": '0', "Content-Type": "application/json", "Accept-Encoding": "deflate"}
        params = {}
        resource = "/sqlinstance"
        body = six.b(json.dumps({"cu":sql_instance,'useAsDefault':useAsDefault}))
        (resp, header) = self._send("POST", project_name, body, resource, params, headers)
        return CreateSqlInstanceResponse(header, resp)

    def update_sql_instance(self, project_name, sql_instance,useAsDefault):
        """ update sql instance config
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type sql_instance: long
        :param sql_instance: the sql instance config

        :return: CreateSqlInstanceResponse

        :raise: LogException
        """
        headers = {"x-log-bodyrawsize": '0', "Content-Type": "application/json", "Accept-Encoding": "deflate"}
        params = {}
        resource = "/sqlinstance"
        body = six.b(json.dumps({"cu": sql_instance,'useAsDefault':useAsDefault}))
        # headers["Host"] = "ali-cn-hangzhou-sls-admin.cn-hangzhou.log.aliyuncs.com"
        (resp, header) = self._send("PUT", project_name, body, resource, params, headers)
        return UpdateSqlInstanceResponse(header, resp)

    def list_sql_instance(self, project_name):
        """ list the sql instance config
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :return: ListSqlInstanceResponse

        :raise: LogException
        """
        headers = {}
        params = {}
        resource = "/sqlinstance"
        (resp, header) = self._send("GET", project_name, None, resource, params, headers)
        return ListSqlInstanceResponse(header, resp)

    def create_resource(self, resource):
        """create resource
        Unsuccessful operation will cause an LogException.
        resource options: resource_name resource_type  [schema:List[ResourceSchemaItem],description,ext_info]

        :type resource: Resource
        :param resource: instance of Resource
        """
        if not isinstance(resource, Resource):
            raise TypeError("record must be instance of Resource ")
        params = {}
        resource.check_for_create()
        body = {
            "name": resource.get_resource_name(),
            "type": resource.get_resource_type(),
        }
        description = resource.get_description()
        schema_list = resource.get_schema()
        acl = resource.get_acl()
        ext_info = resource.get_ext_info()
        if description:
            body["description"] = description
        if schema_list:
            body["schema"] = json.dumps({"schema": [schema.to_dict() for schema in schema_list]})
        if acl:
            body["acl"] = json.dumps(acl)
        if ext_info:
            body["extInfo"] = ext_info
        body_str = six.b(json.dumps(body))
        headers = {'x-log-bodyrawsize': str(len(body_str))}
        resource = "/resources"
        (resp, header) = self._send("POST", None, body_str, resource, params, headers)
        return CreateResourceResponse(header, resp)

    def delete_resource(self, resource_name):
        """delete resource
        Unsuccessful operation will cause an LogException.

        :type resource_name: string
        :param resource_name: resource name
        """
        if not isinstance(resource_name, str):
            raise TypeError("resource_name type must be str")
        headers = {}
        params = {}
        resource = "/resources/" + resource_name
        (resp, header) = self._send("DELETE", None, None, resource, params, headers)
        return DeleteResourceResponse(header, resp)

    def update_resource(self, resource):
        """update resource
        Unsuccessful operation will cause an LogException.
        resource options: resource_name [schema,description,ext_info]

        :type resource: Resource
        :param resource: instance of Resource
        """
        if not isinstance(resource, Resource):
            raise TypeError("resource type must be Resource instance")
        params = {}
        body = {}
        resource.check_for_update()
        description = resource.get_description()
        schema_list = resource.get_schema()
        ext_info = resource.get_ext_info()
        acl = resource.get_acl()
        if schema_list is not None:
            body["schema"] = json.dumps({"schema": [schema.to_dict() for schema in schema_list]})
        if description is not None:
            body["description"] = description
        if ext_info is not None:
            body["extInfo"] = ext_info
        if acl:
            body["acl"] = json.dumps(acl)
        body_str = six.b(json.dumps(body))
        headers = {'x-log-bodyrawsize': str(len(body_str))}
        resource = "/resources/" + resource.get_resource_name()
        (resp, header) = self._send("PUT", None, body_str, resource, params, headers)
        return UpdateResourceResponse(header, resp)

    def get_resource(self, resource_name):
        """get resource
        Unsuccessful operation will cause an LogException.

        :type resource_name: string
        :param resource_name: resource name
        """
        if not isinstance(resource_name, str):
            raise TypeError("resource_name type must be str")
        headers = {}
        params = {}
        resource = "/resources/" + resource_name
        (resp, header) = self._send("GET", None, None, resource, params, headers)
        return GetResourceResponse(header, resp)

    def list_resources(self, offset=0, size=100, resource_type=None, resource_names=None):
        """ list resources
        Unsuccessful operation will cause an LogException.

        :type offset: int
        :param offset: line offset of return resources

        :type size: int
        :param size: max line number of return resources

        :type resource_type: string
        :param resource_type: resource type

        :type resource_names: list
        :param resource_names: resource names witch need to be listed

        :return: ListResourcesResponse
        :raise: LogException
        """
        if resource_type and not isinstance(resource_type, str):
            raise TypeError("resource_type type must be str")
        if resource_names and not isinstance(resource_names, list):
            raise TypeError("resource_names type must be list")
        if not (isinstance(size, int) and isinstance(offset, int)):
            raise TypeError("size and offset type must be int")
        headers = {}
        params = {"offset": offset, "size": size}
        if resource_names:
            params["names"] = ",".join(resource_names)
        if resource_type:
            params["type"] = resource_type
        resource = "/resources"
        (resp, header) = self._send("GET", None, None, resource, params, headers)
        return ListResourcesResponse(resp, header)

    def create_resource_record(self, resource_name, record):
        """create resource record
        Unsuccessful operation will cause an LogException.

        :type resource_name: string
        :param resource_name: resource name

        :type record: ResourceRecord
        :param record: record options: value,[record_id,tag]
        """
        if not isinstance(record, ResourceRecord):
            raise TypeError("resource type must be ResourceRecord instance")
        if not isinstance(resource_name, str):
            raise TypeError("resource_name type must be str")
        params = {}
        resource = "/resources/" + resource_name + "/records"
        record.check_value()
        body = {"value": json.dumps(record.get_value())}
        record_id = record.get_record_id()
        tag = record.get_tag()
        if record_id:
            body["id"] = record_id
        if tag:
            body["tag"] = tag
        body_str = six.b(json.dumps(body))
        headers = {'x-log-bodyrawsize': str(len(body_str))}
        (resp, header) = self._send("POST", None, body_str, resource, params, headers)
        return CreateRecordResponse(header, resp)

    def delete_resource_record(self, resource_name, record_ids):
        """delete resource record
        Unsuccessful operation will cause an LogException.

        :type resource_name: string
        :param resource_name: resource name

        :type record_ids: list
        :param record_ids: record id list which need to be deleted
        """
        if not isinstance(resource_name, str):
            raise TypeError("resource_name type must be str")
        if not isinstance(record_ids, list):
            raise TypeError("record_ids type must be list,element is record id which type is str")
        headers = {}
        params = {"ids": ",".join(record_ids)}
        resource = "/resources/" + resource_name + "/records"
        (resp, header) = self._send("DELETE", None, None, resource, params, headers)
        return DeleteRecordResponse(header, resp)

    def update_resource_record(self, resource_name, record):
        """update resource record
        Unsuccessful operation will cause an LogException.

        :type resource_name: string
        :param resource_name: resource name

        :type record: ResourceRecord
        :param record: record options: value,record_id,[tag]

        """
        if not isinstance(record, ResourceRecord):
            raise TypeError("resource type must be instance of ResourceRecord")
        if not isinstance(resource_name, str):
            raise TypeError("resource_name type must be str")
        params = {}
        record.check_for_update()
        body = {"value": json.dumps(record.get_value())}
        record_id = record.get_record_id()
        tag = record.get_tag()
        if tag is not None:
            body["tag"] = tag
        body_str = six.b(json.dumps(body))
        headers = {'x-log-bodyrawsize': str(len(body_str))}
        resource = "/resources/" + resource_name + "/records/" + record_id
        (resp, header) = self._send("PUT", None, body_str, resource, params, headers)
        return UpdateRecordResponse(header, resp)

    def upsert_resource_record(self, resource_name, records):
        """upsert resource record
        Unsuccessful operation will cause an LogException.

        :type resource_name: string
        :param resource_name: resource name

        :type records: list[ResourceRecord]
        :param records: if record is exist,doUpdate,else doInsert
        """
        if not isinstance(records, list):
            raise TypeError("resource type must be list ,element is instance of ResourceRecord")
        if not isinstance(resource_name, str):
            raise TypeError("resource_name type must be str")
        if not records:
            raise ValueError("records is not be empty or None")
        record_list = []
        for record in records:
            record.check_value()
            record_dict = record.to_dict()
            record_dict["value"] = json.dumps(record_dict.get("value"))
            record_list.append(record_dict)
        params = {}
        body = {"records": record_list}
        body_str = six.b(json.dumps(body))
        headers = {'x-log-bodyrawsize': str(len(body_str))}
        resource = "/resources/" + resource_name + "/records"
        (resp, header) = self._send("PUT", None, body_str, resource, params, headers)
        return UpsertRecordResponse(header, resp)

    def get_resource_record(self, resource_name, record_id):
        """get resource record
        Unsuccessful operation will cause an LogException.

        :type resource_name: string
        :param resource_name: resource name

        :type record_id: string
        :param record_id: record id
        """
        if not isinstance(resource_name, str):
            raise TypeError("resource_name type must be str")
        if not isinstance(record_id, str):
            raise TypeError("record_id type must be str")
        headers = {}
        params = {}
        resource = "/resources/" + resource_name + "/records/" + record_id
        (resp, header) = self._send("GET", None, None, resource, params, headers)
        return GetRecordResponse(header, resp)

    def list_resource_records(self, resource_name, tag=None, record_ids=None, offset=0, size=100):
        """list resource records
        Unsuccessful operation will cause an LogException.

        :type resource_name: string
        :param resource_name: resource name

        :type tag: string
        :param tag: record tag

        :type offset: long int
        :param offset: start location

        :type size: long int
        :param size: max records for each page

        :type record_ids: list
        :param record_ids: record id list witch need to be listed
        """
        if tag and not isinstance(tag, str):
            raise TypeError("tag type must be str")
        if not isinstance(resource_name, str):
            raise TypeError("resource_name type must be str")
        if not (isinstance(size, int) and isinstance(offset, int)):
            raise TypeError("size and offset type must be int")
        if record_ids and not isinstance(record_ids, list):
            raise TypeError("record_ids type must be list,element is record id which type is str")
        headers = {}
        params = {"offset": offset, "size": size}
        if tag is not None:
            params["tag"] = tag
        if record_ids:
            params["ids"] = ','.join(record_ids)
        resource = "/resources/" + resource_name + "/records"
        (resp, header) = self._send("GET", None, None, resource, params, headers)
        return ListRecordResponse(resp, header)

    def create_topostore(self, topostore):
        """create topostore
        Unsuccessful operation will cause an LogException.

        :type topostore: Topostore
        :param topostore: instance of Topostore
        """
        if not isinstance(topostore, Topostore):
            raise TypeError("topostore must be instance of Topostore ")
        params = {}
        topostore.check_for_create()
        body = {
            "name": topostore.get_name(),
        }
        tag = topostore.get_tag()
        description = topostore.get_description()
        schema = topostore.get_schema()
        acl = topostore.get_acl()
        ext_info = topostore.get_ext_info()
        if description:
            body["description"] = description

        if schema:
            body["schema"] = json.dumps(schema)

        if tag:
            body["tag"] = json.dumps(tag)

        if acl:
            body["acl"] = json.dumps(acl)

        if ext_info:
            body["extInfo"] = ext_info

        body_str = six.b(json.dumps(body))
        headers = {'x-log-bodyrawsize': str(len(body_str))}
        resource = "/topostores"
        (resp, header) = self._send("POST", None, body_str, resource, params, headers)
        return CreateTopostoreResponse(header, resp)

    def update_topostore(self, topostore):
        """update topostore
        Unsuccessful operation will cause an LogException.

        :type topostore: Topostore
        :param topostore: instance of Topostore
        """
        if not isinstance(topostore, Topostore):
            raise TypeError("topostore must be instance of Topostore ")
        params = {}
        topostore.check_for_create()
        body = {
            "name": topostore.get_name(),
        }
        tag = topostore.get_tag()
        description = topostore.get_description()
        schema = topostore.get_schema()
        acl = topostore.get_acl()
        ext_info = topostore.get_ext_info()
        if description:
            body["description"] = description
        if schema:
            body["schema"] = json.dumps(schema)
        if tag:
            body["tag"] = json.dumps(tag)
        if acl:
            body["acl"] = json.dumps(acl)
        if ext_info:
            body["extInfo"] = ext_info
        body_str = six.b(json.dumps(body))
        headers = {'x-log-bodyrawsize': str(len(body_str))}
        resource = "/topostores/" + topostore.get_name()
        (resp, header) = self._send("PUT", None, body_str, resource, params, headers)
        return UpdateTopostoreResponse(header, resp)

    def delete_topostore(self, topostore_name):
        """delete topostore
        Unsuccessful operation will cause an LogException.

        :type topostore_name: string
        :param topostore_name: resource name
        """
        if not isinstance(topostore_name, str):
            raise TypeError("topostore_name type must be str")
        headers = {}
        params = {}
        resource = "/topostores/" + topostore_name
        (resp, header) = self._send("DELETE", None, None, resource, params, headers)
        return DeleteTopostoreResponse(header, resp)

    def get_topostore(self, topostore_name):
        """get topostore
        Unsuccessful operation will cause an LogException.

        :type topostore_name: string
        :param topostore_name: topostore name
        """
        if not isinstance(topostore_name, str):
            raise TypeError("topostore_name type must be str")
        headers = {}
        params = {}
        resource = "/topostores/" + topostore_name
        (resp, header) = self._send("GET", None, None, resource, params, headers)
        return GetTopostoreResponse(header, resp)

    def list_topostores(self, names=None, tag_key=None, tag_value=None, offset=0, size=100):
        """ list topostores
        Unsuccessful operation will cause an LogException.

        :type offset: int
        :param offset: line offset of return topostores

        :type size: int
        :param size: max line number of return topostores

        :type tag_key: string
        :param tag_key: topostore tag key

        :type tag_value: string
        :param tag_value: topostore tag value

        :type names: list
        :param names: topostore names witch need to be listed

        :return: ListTopostoresResponse
        :raise: LogException
        """
        if names and not isinstance(names, list):
            raise TypeError("topostore_names type must be list")

        if tag_key and not isinstance(tag_key, str):
            raise TypeError("tag_key type must be str")

        if tag_value and not isinstance(tag_value, str):
            raise TypeError("tag_value type must be str")

        if not (isinstance(size, int) and isinstance(offset, int)):
            raise TypeError("size and offset type must be int")

        headers = {}
        params = {"offset": offset, "size": size}
        if names:
            params["names"] = ",".join(names)
        if tag_key:
            params["tagKey"] = tag_key
        if tag_value:
            params["tagValue"] = tag_value
        resource = "/topostores"
        (resp, header) = self._send("GET", None, None, resource, params, headers)
        return ListTopostoresResponse(resp, header)

    def create_topostore_node(self, topostore_name, node):
        """create topostore node
        Unsuccessful operation will cause an LogException.

        :type node: TopostoreNode
        :param node: instance of TopostoreNode
        """
        if not isinstance(topostore_name, str):
            raise TypeError("topostore_name type must be str")
        if not isinstance(node, TopostoreNode):
            raise TypeError("node must be instance of TopostoreNode ")
        params = {}
        node.check_for_create()
        body = node.to_dict()
        property = node.get_property()
        if property:
            body['property'] = json.dumps(property)
        body_str = six.b(json.dumps(body))
        headers = {'x-log-bodyrawsize': str(len(body_str))}
        resource = "/topostores/" + topostore_name + "/nodes"
        (resp, header) = self._send("POST", None, body_str, resource, params, headers)
        return CreateTopostoreNodeResponse(header, resp)

    def upsert_topostore_node(self, topostore_name, nodes):
        """upsert topostore node
        Unsuccessful operation will cause an LogException.

        :type nodes: list of TopostoreNode
        :param nodes: list of TopostoreNode
        """
        if not isinstance(topostore_name, str):
            raise TypeError("topostore_name type must be str")
        if not isinstance(nodes, list):
            raise TypeError("nodes must be instance of list ")

        nodes_list = []
        if len(nodes) > 100:
            raise Exception("nodes count should less than 100")

        for node in nodes:
            if not isinstance(node, TopostoreNode):
                raise TypeError("node item  must be instance of TopostoreNode ")
            node.check_for_create()
            node_dict = node.to_dict()
            property = node.get_property()
            if property:
                node_dict['property'] = json.dumps(property)
            nodes_list.append(node_dict)
        params = {}
        body = {"nodes" :nodes_list }
        body_str = six.b(json.dumps(body))
        headers = {'x-log-bodyrawsize': str(len(body_str))}
        resource = "/topostores/" + topostore_name + "/nodes"
        (resp, header) = self._send("PUT", None, body_str, resource, params, headers)
        return UpsertTopostoreNodeResponse(header, resp)

    def update_topostore_node(self, topostore_name, node):
        """update topostore node
        Unsuccessful operation will cause an LogException.

        :type node: TopostoreNode
        :param node: instance of TopostoreNode
        """
        if not isinstance(topostore_name, str):
            raise TypeError("topostore_name type must be str")
        if not isinstance(node, TopostoreNode):
            raise TypeError("node must be instance of TopostoreNode ")
        params = {}
        node.check_for_create()
        body = node.to_dict()
        property = node.get_property()
        if property:
            body['property'] = json.dumps(property)
        body_str = six.b(json.dumps(body))
        headers = {'x-log-bodyrawsize': str(len(body_str))}
        resource = "/topostores/"  + topostore_name + "/nodes/" + node.get_node_id()
        (resp, header) = self._send("PUT", None, body_str, resource, params, headers)
        return UpdateTopostoreNodeResponse(header, resp)

    def delete_topostore_node(self, topostore_name, node_ids):
        """delete topostore node
        Unsuccessful operation will cause an LogException.

        :type topostore_name: string
        :param topostore_name: topostore_name name

        :type node_ids: list
        :param node_ids: topostore node id list, id should be str
        """
        if not isinstance(topostore_name, str):
            raise TypeError("resource_name type must be str")
        if not isinstance(node_ids, list):
            raise TypeError("node_ids type must be list of str")

        for node_id in node_ids:
            if not isinstance(node_id, str):
                raise TypeError("node_id type must be str")
        params = {"nodeIds" : ",".join(node_ids)}
        body = {}
        body_str = six.b(json.dumps(body))
        headers = {'x-log-bodyrawsize': str(len(body_str))}
        resource = "/topostores/" + topostore_name + "/nodes"
        (resp, header) = self._send("DELETE", None, body_str, resource, params, headers)
        return DeleteTopostoreNodeResponse(header, resp)

    def get_topostore_node(self, topostore_name, node_id):
        """get topsotore node
        Unsuccessful operation will cause an LogException.

        :type topostore_name: string
        :param topostore_name: topostore_name name

        :type node_id: string
        :param node_id: topostore node id
        """
        if not isinstance(topostore_name, str):
            raise TypeError("resource_name type must be str")
        if not isinstance(node_id, str):
            raise TypeError("node_id type must be str, got %s" % type(node_id))
        headers = {}
        params = {}
        resource = "/topostores/" + topostore_name + "/nodes/" + node_id
        (resp, header) = self._send("GET", None, None, resource, params, headers)
        return GetTopostoreNodeResponse(header, resp)

    def list_topostore_node(self, topostore_name, node_ids=None, node_types=None, property_key=None,
                property_value=None, offset=0, size=100):
        """list Topostore nodes
        Unsuccessful operation will cause an LogException.

        :type topostore_name: string
        :param topostore_name: topostore name

        :type node_ids: list of string
        :param node_ids: topostore node ids

        :type node_types: list of string
        :param node_types: list of node id(which is str)

        :type property_key: string
        :param property_key: property_key of node id property

        :type property_value: string
        :param property_value: property_value of node id property

        :type offset: long int
        :param offset: start location

        :type size: long int
        :param size: max nodes for each page

        """
        if not isinstance(topostore_name, str):
            raise TypeError("resource_name type must be str")

        if node_ids and not isinstance(node_ids, list):
            raise TypeError("node_ids type must be list,element is node id which type is str")

        if node_types and not isinstance(node_types, list):
            raise TypeError("node_types must be list of string")

        if node_types and isinstance(node_types, list):
            for node_type in node_types:
                if not isinstance(node_type, str):
                    raise TypeError("node_types must be list of string")

        if property_key and not isinstance(property_key, str):
            raise TypeError("property_key must be str")

        if property_value and not isinstance(property_value, str):
            raise TypeError("property_value must be str")

        if not (isinstance(size, int) and isinstance(offset, int)):
            raise TypeError("size and offset type must be int")

        headers = {}
        params = {"offset": offset, "size": size}
        if node_types is not None:
            params["nodeTypes"] = ','.join(node_types)
        if property_key is not None:
            params["propertyKey"] = property_key
        if property_value is not None:
            params["propertyValue"] = property_value
        if node_ids is not None:
            params["nodeIds"] = ','.join(node_ids)
        resource = "/topostores/" + topostore_name + "/nodes"
        (resp, header) = self._send("GET", None, None, resource, params, headers)
        return ListTopostoreNodesResponse(resp, header)

    def create_topostore_relation(self, topostore_name, relation):
        """create topostore relation
        Unsuccessful operation will cause an LogException.

        :type relation: TopostoreRelation
        :param relation: instance of TopostoreRelation
        """
        if not isinstance(topostore_name, str):
            raise TypeError("topostore_name type must be str")
        if not isinstance(relation, TopostoreRelation):
            raise TypeError("relation must be instance of TopostoreRelation ")
        params = {}
        relation.check_for_create()
        body = relation.to_dict()
        property = relation.get_property()
        if property:
            body['property'] = json.dumps(property)
        body_str = six.b(json.dumps(body))
        headers = {'x-log-bodyrawsize': str(len(body_str))}
        resource = "/topostores/" + topostore_name + "/relations"
        (resp, header) = self._send("POST", None, body_str, resource, params, headers)
        return CreateTopostoreRelationResponse(header, resp)

    def upsert_topostore_relation(self, topostore_name, relations):
        """upsert topostore relation
        Unsuccessful operation will cause an LogException.

        :type relations: list of TopostoreRelation
        :param relations: list of TopostoreRelation
        """
        if not isinstance(topostore_name, str):
            raise TypeError("topostore_name type must be str")
        if not isinstance(relations, list):
            raise TypeError("relations must be instance of list ")

        relation_list = []
        if len(relations) > 100:
            raise Exception("relations count should less than 100")

        for relation in relations:
            if not isinstance(relation, TopostoreRelation):
                raise TypeError("node item  must be instance of TopostoreRelation ")
            relation.check_for_create()
            relation_dict = relation.to_dict()
            property = relation.get_property()
            if property:
                relation_dict['property'] = json.dumps(property)
            relation_list.append(relation_dict)
        params = {}
        body = {"relations" :relation_list }
        body_str = six.b(json.dumps(body))
        headers = {'x-log-bodyrawsize': str(len(body_str))}
        resource = "/topostores/" + topostore_name + "/relations"
        (resp, header) = self._send("PUT", None, body_str, resource, params, headers)
        return UpsertTopostoreRelationResponse(header, resp)

    def update_topostore_relation(self, topostore_name, relation):
        """update topostore relation
        Unsuccessful operation will cause an LogException.

        :type relation: TopostoreRelation
        :param relation: instance of TopostoreRelation
        """
        if not isinstance(topostore_name, str):
            raise TypeError("topostore_name type must be str")
        if not isinstance(relation, TopostoreRelation):
            raise TypeError("relation must be instance of TopostoreRelation ")
        params = {}
        relation.check_for_create()
        body = relation.to_dict()
        property = relation.get_property()
        if property:
            body['property'] = json.dumps(property)
        body_str = six.b(json.dumps(body))
        headers = {'x-log-bodyrawsize': str(len(body_str))}
        resource = "/topostores/" + topostore_name + "/relations/" + relation.get_relation_id()
        (resp, header) = self._send("PUT", None, body_str, resource, params, headers)
        return UpdateTopostoreRelationResponse(header, resp)

    def delete_topostore_relation(self, topostore_name, relation_ids):
        """delete topostore relation
        Unsuccessful operation will cause an LogException.

        :type topostore_name: string
        :param topostore_name: topostore_name name

        :type relation_ids: list of string
        :param relation_ids: topostore relation ids
        """
        if not isinstance(topostore_name, str):
            raise TypeError("resource_name type must be str")
        if not isinstance(relation_ids, list):
            raise TypeError("relation_ids type must be str")
        for relation_id in relation_ids:
            if not isinstance(relation_id, str):
                raise TypeError("relation_id type must be str")
        params = {"relationIds" :  ",".join(relation_ids)}
        body = {}
        body_str = six.b(json.dumps(body))
        headers = {'x-log-bodyrawsize': str(len(body_str))}
        resource = "/topostores/" + topostore_name + "/relations"
        (resp, header) = self._send("DELETE", None, body_str, resource, params, headers)
        return DeleteTopostoreRelationResponse(header, resp)

    def get_topostore_relation(self, topostore_name, relation_id):
        """get topsotore relation
        Unsuccessful operation will cause an LogException.

        :type topostore_name: string
        :param topostore_name: topostore_name name

        :type relation_id: string
        :param relation_id: topostore relation id
        """
        if not isinstance(topostore_name, str):
            raise TypeError("topostore_name type must be str")
        if not isinstance(relation_id, str):
            raise TypeError("relation_id type must be str")
        headers = {}
        params = {}
        resource = "/topostores/" + topostore_name + "/relations/" + relation_id
        (resp, header) = self._send("GET", None, None, resource, params, headers)
        return GetTopostoreRelationResponse(header, resp)

    def list_topostore_relation(self, topostore_name, relation_ids=None, relation_types=None,
            src_node_ids=None, dst_node_ids=None,
            property_key=None, property_value=None, offset=0, size=100):
        """list Topostore relationss
        Unsuccessful operation will cause an LogException.

        :type topostore_name: string
        :param topostore_name: topostore name

        :type relation_ids: list of string
        :param relation_ids: topostore relation ids (which is str)

        :type relation_types: list of string
        :param relation_types: list of relation id (which is str)

        :type src_node_ids: list of string
        :param src_node_ids: list of src_node_id (which is str)

        :type dst_node_ids: list of string
        :param dst_node_ids: list of dst_node_id (which is str)

        :type property_key: string
        :param property_key: property_key of relation id property

        :type property_value: string
        :param property_value: property_value of relation id property

        :type offset: long int
        :param offset: start location

        :type size: long int
        :param size: max relations for each page

        """
        if not isinstance(topostore_name, str):
            raise TypeError("resource_name type must be str")

        if relation_ids and not isinstance(relation_ids, list):
            raise TypeError("relation_ids type must be list,element is relation id which type is str")

        if relation_types and not isinstance(relation_types, list):
            raise TypeError("relation_types must be list of string")

        if relation_types and isinstance(relation_types, list):
            for relation_type in relation_types:
                if not isinstance(relation_type, str):
                    raise TypeError("relation_types must be list of string")

        if src_node_ids and not isinstance(src_node_ids, list):
            raise TypeError("src_node_ids must be list of string")
        if src_node_ids and isinstance(src_node_ids, list):
            for src_node_id in src_node_ids:
                if not isinstance(src_node_id, str):
                    raise TypeError("src_node_ids must be list of string")

        if dst_node_ids and not isinstance(dst_node_ids, list):
            raise TypeError("dst_node_id must be str")
        if dst_node_ids and isinstance(dst_node_ids, list):
            for dst_node_id in dst_node_ids:
                if not isinstance(dst_node_id, str):
                    raise TypeError("dst_node_ids must be list of string")

        if property_key and not isinstance(property_key, str):
            raise TypeError("property_key must be str")

        if property_value and not isinstance(property_value, str):
            raise TypeError("property_value must be str")

        if not (isinstance(size, int) and isinstance(offset, int)):
            raise TypeError("size and offset type must be int")

        headers = {}
        params = {"offset": offset, "size": size}
        if relation_ids is not None:
            params["relationIds"] = ','.join(relation_ids)

        if relation_types is not None:
            params["relationTypes"] = ",".join(relation_types)

        if src_node_ids is not None:
            params["srcNodeIds"] = ",".join(src_node_ids)

        if dst_node_ids is not None:
            params["dstNodeIds"] = ",".join(dst_node_ids)

        if property_key is not None:
            params["propertyKey"] = property_key

        if property_value is not None:
            params["propertyValue"] = property_value

        resource = "/topostores/" + topostore_name + "/relations"
        (resp, header) = self._send("GET", None, None, resource, params, headers)
        return ListTopostoreRelationsResponse(resp, header)

    def create_export(self, project_name, export):
        """ Create an export job
        Unsuccessful opertaion will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type export: Export
        :param export: the export job configuration
        """
        params = {}
        export_sink_config = export.getConfiguration().getSink()
        export_type = export_sink_config.getType()
        if export_type not in ["AliyunODPS", "AliyunOSS"]:
            raise TypeError("export job type must in AliyunODPS or AliyunOSS, not %s" % export_type)
        if export_type == "AliyunODPS":
            sink = maxcompute_sink_deserialize(export_sink_config)
        elif export_type == "AliyunOSS":
            sink = oss_sink_deserialize(export_sink_config)
        export = export_deserialize(export, sink)
        body = six.b(export)
        headers = {'Content-Type': 'application/json', 'x-log-bodyrawsize': str(len(body))}
        resource = "/jobs"
        (resp, header) = self._send("POST", project_name, body, resource, params, headers)
        return CreateExportResponse(header, resp)

    def delete_export(self, project_name, job_name):
        """ Create an export job
        Unsuccessful opertaion will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type job_name: string
        :param job_name: the job name of export job
        """
        params = {}
        headers = {}
        resource = "/jobs/" + job_name
        (resp, header) = self._send("DELETE", project_name, None, resource, params, headers)
        return DeleteExportResponse(header, resp)

    def update_export(self, project_name, job_name, export):
        """ Update and Restart an export job
        Unsuccessful opertaion will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type job_name: string
        :param job_name: the job name of export job

        :type export: string
        :param export: the export job configuration
        """
        if not isinstance(export, str):
            raise TypeError("export type must be string")
        params = {"action": "RESTART"}
        body = six.b(export)
        headers = {'Content-Type': 'application/json', 'x-log-bodyrawsize': str(len(body))}
        resource = "/jobs/" + job_name
        (resp, header) = self._send("PUT", project_name, body, resource, params, headers)
        return UpdateExportResponse(header, resp)

    def get_export(self, project_name, job_name):
        """ get export
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Project name

        :type name: string
        :param name: the export name

        :return: GetExportResponse
        :raise: LogException
        """
        params = {}
        headers = {}
        resource = "/jobs/" + job_name
        (resp, header) = self._send("GET", project_name, None, resource, params, headers)
        return GetExportResponse(header, resp)

    def list_export(self, project_name, offset=0, size=100):
        """ list exports
        Unsuccessful operation will cause an LogException.

        :type project_name: string
        :param project_name: the Pqroject name

        :type offset: int
        :param offset: line offset of return logs

        :type size: int
        :param size: max line number of return logs, -1 means get all

        :return: ListExportsResponse
        :raise: LogException
        """
        # need to use extended method to get more
        if int(size) == -1 or int(size) > MAX_LIST_PAGING_SIZE:
            return list_more(self.list_export, int(offset), int(size), MAX_LIST_PAGING_SIZE,
                             project_name)
        headers = {}
        params = {}
        resource = '/jobs'
        params['offset'] = str(offset)
        params['size'] = str(size)
        params['jobType'] = "Export"
        (resp, header) = self._send("GET", project_name, None, resource, params, headers)
        return ListExportResponse(resp, header)

    def list_alert(self, project, offset=0, size=100):
        """list alerts
        Unsuccessful opertaion will cause an LogException.

        :type project: string
        :param project: the project name

        :type offset: int
        :param offset: the offset of all the matched alerts

        :type size: int
        :param size: the max return alert count, -1 means all

        :return: ListEntityResponse

        :raise: LogException
        """
        # need to use extended method to get more
        if int(size) == -1 or int(size) > MAX_LIST_PAGING_SIZE:
            return list_more(self.list_alert, int(offset), int(size), MAX_LIST_PAGING_SIZE,
                             project)

        headers = {}
        params = {}
        resource = "/jobs"
        params["offset"] = str(offset)
        params["size"] = str(size)
        params['jobType'] = "Alert"
        (resp, header) = self._send("GET", project, None, resource, params, headers)
        return ListEntityResponse(header, resp, resource_name="alerts", entities_key="results")

    def get_alert(self, project, entity):
        """get alert
        Unsuccessful opertaion will cause an LogException.

        :type project: string
        :param project: the project name

        :type entity: string
        :param entity: the alert name

        :return: GetEntityResponse

        :raise: LogException
        """
        headers = {}
        params = {}
        resource = "/jobs/" + entity
        (resp, header) = self._send("GET", project, None, resource, params, headers)
        return GetEntityResponse(header, resp)

    def delete_alert(self, project, entity):
        """delte alert
        Unsuccessful opertaion will cause an LogException.

        :type project: string
        :param project: the project name

        :type entity: string
        :param entity: the alert name

        :return: DeleteEntityResponse

        :raise: LogException
        """
        headers = {}
        params = {}
        resource = "/jobs/" + entity
        (resp, header) = self._send("DELETE", project, None, resource, params, headers)
        return DeleteEntityResponse(header, resp)

    def update_alert(self, project, detail):
        """update alert
        Unsuccessful opertaion will cause an LogException.

        :type project: string
        :param project: the project name

        :type detail: dict/string
        :param detail: json string of alert config details

        :return: UpdateEntityResponse

        :raise: LogException
        """
        params = {}
        headers = {}
        alert_name = None
        if hasattr(detail, "to_json"):
            detail = detail.to_json()
            body_str = six.b(json.dumps(detail))
            alert_name = detail.get("name", "")
        elif isinstance(detail, six.binary_type):
            body_str = detail
        elif isinstance(detail, six.text_type):
            body_str = detail.encode("utf8")
        else:
            body_str = six.b(json.dumps(detail))
            alert_name = detail.get("name", "")

        if alert_name is None:
            alert_name = json.loads(body_str).get("name", "")

        assert alert_name, LogException(
            "InvalidParameter",
            'unknown alert name in "{0}"'.format(detail),
        )

        headers["Content-Type"] = "application/json"
        headers["x-log-bodyrawsize"] = str(len(body_str))
        resource = "/jobs/" + alert_name
        (resp, headers) = self._send("PUT", project, body_str, resource, params, headers)
        return UpdateEntityResponse(headers, resp)

    def create_alert(self, project, detail):
        """create alert
        Unsuccessful opertaion will cause an LogException.

        :type project: string
        :param project: the project name

        :type detail: dict/string
        :param detail: json string of alert config details

        :return: CreateEntityResponse

        :raise: LogException
        """

        params = {}
        headers = {"x-log-bodyrawsize": "0", "Content-Type": "application/json"}

        if hasattr(detail, "to_json"):
            detail = detail.to_json()
            body_str = six.b(json.dumps(detail))
        elif isinstance(detail, six.binary_type):
            body_str = detail
        elif isinstance(detail, six.text_type):
            body_str = detail.encode("utf8")
        else:
            body_str = six.b(json.dumps(detail))

        resource = "/jobs"
        (resp, header) = self._send("POST", project, body_str, resource, params, headers)
        return CreateEntityResponse(header, resp)

    def list_dashboard(self, project, offset=0, size=100):
        """list the dashboards
        Unsuccessful opertaion will cause an LogException.

        :type project: string
        :param project: the project name

        :type offset: int
        :param offset: the offset of all the matched dashboards

        :type size: int
        :param size: the max return dashboard count, -1 means all

        :return: ListEntityResponse

        :raise: LogException
        """
        if int(size) == -1 or int(size) > MAX_LIST_PAGING_SIZE:
            return list_more(self.list_dashboard, int(offset), int(size), MAX_LIST_PAGING_SIZE,
                             project)

        headers = {}
        params = {}
        resource = "/" + pluralize("dashboard")
        params["offset"] = str(offset)
        params["size"] = str(size)
        (resp, header) = self._send("GET", project, None, resource, params, headers)
        return ListEntityResponse(header, resp, resource_name="dashboards", entities_key="dashboards")

    def get_dashboard(self, project, entity):
        """get dashboard
        Unsuccessful opertaion will cause an LogException.

        :type project: string
        :param project: the project name

        :type entity: string
        :param entity: the dashabord name

        :return: GetEntityResponse

        :raise: LogException
        """
        headers = {}
        params = {}
        resource = "/" + pluralize("dashboard") + "/" + entity
        (resp, header) = self._send("GET", project, None, resource, params, headers)
        return GetEntityResponse(header, resp)

    def delete_dashboard(self, project, entity):
        """delete dashboard
        Unsuccessful opertaion will cause an LogException.

        :type project: string
        :param project: the project name

        :type entity: string
        :param entity: the dashabord name

        :return: DeleteEntityResponse

        :raise: LogException
        """
        headers = {}
        params = {}
        resource = "/" + pluralize("dashboard") + "/" + entity
        (resp, header) = self._send("DELETE", project, None, resource, params, headers)
        return DeleteEntityResponse(header, resp)

    def update_dashboard(self, project, detail):
        """update dashboard
        Unsuccessful opertaion will cause an LogException.

        :type project: string
        :param project: the project name

        :type detail: dict/string
        :param detail: json string of dashboard config details

        :return: UpdateEntityResponse

        :raise: LogException
        """
        params = {}
        headers = {}
        dashabord_name = None
        if hasattr(detail, "to_json"):
            detail = detail.to_json()
            body_str = six.b(json.dumps(detail))
            dashabord_name = detail.get("dashboardName" or "name", "")
        elif isinstance(detail, six.binary_type):
            body_str = detail
        elif isinstance(detail, six.text_type):
            body_str = detail.encode("utf8")
        else:
            body_str = six.b(json.dumps(detail))
            dashabord_name = detail.get("dashboardName" or "name", "")

        if dashabord_name is None:
            dashabord_name = json.loads(body_str).get("dashboardName", "")

        assert dashabord_name, LogException(
            "InvalidParameter",
            'unknown dashabord name in "{0}"'.format(detail),
        )
        headers["Content-Type"] = "application/json"
        headers["x-log-bodyrawsize"] = str(len(body_str))
        resource = "/" + pluralize("dashboard") + "/" + dashabord_name
        (resp, headers) = self._send("PUT", project, body_str, resource, params, headers)
        return UpdateEntityResponse(headers, resp)

    def create_dashboard(self, project, detail):
        """create dashboard
        Unsuccessful opertaion will cause an LogException.

        :type project: string
        :param project: the project name

        :type detail: dict/string
        :param detail: json string of dashboard config details

        :return: CreateEntityResponse

        :raise: LogException
        """

        params = {}
        headers = {"x-log-bodyrawsize": "0", "Content-Type": "application/json"}

        if hasattr(detail, "to_json"):
            detail = detail.to_json()
            body_str = six.b(json.dumps(detail))
        elif isinstance(detail, six.binary_type):
            body_str = detail
        elif isinstance(detail, six.text_type):
            body_str = detail.encode("utf8")
        else:
            body_str = six.b(json.dumps(detail))

        resource = "/" + pluralize("dashboard")
        (resp, header) = self._send("POST", project, body_str, resource, params, headers)
        return CreateEntityResponse(header, resp)

    def list_savedsearch(self, project, offset=0, size=100):
        """list savedsearches
        Unsuccessful opertaion will cause an LogException.

        :type project: string
        :param project: the project name

        :type offset: int
        :param offset: the offset of all the matched savedsearches

        :type size: int
        :param size: the max return savedsearch count, -1 means all

        :return: ListEntityResponse

        :raise: LogException
        """
        if int(size) == -1 or int(size) > MAX_LIST_PAGING_SIZE:
            return list_more(self.list_savedsearch, int(offset), int(size), MAX_LIST_PAGING_SIZE,
                             project)
        headers = {}
        params = {}
        resource = "/" + pluralize("savedsearch")
        params["offset"] = str(offset)
        params["size"] = str(size)
        (resp, header) = self._send("GET", project, None, resource, params, headers)
        return ListEntityResponse(header, resp, resource_name="savedsearches", entities_key="savedsearches")

    def get_savedsearch(self, project, entity):
        """get savedsearch
        Unsuccessful opertaion will cause an LogException.

        :type project: string
        :param project: the project name

        :type entity: string
        :param entity: the savedsearch name

        :return: GetEntityResponse

        :raise: LogException
        """
        headers = {}
        params = {}
        resource = "/" + pluralize("savedsearch") + "/" + entity
        (resp, header) = self._send("GET", project, None, resource, params, headers)
        return GetEntityResponse(header, resp)

    def delete_savedsearch(self, project, entity):
        """delete savedsearch
        Unsuccessful opertaion will cause an LogException.

        :type project: string
        :param project: the project name

        :type entity: string
        :param entity: the savedsearch name

        :return: DeleteEntityResponse

        :raise: LogException
        """
        headers = {}
        params = {}
        resource = "/" + pluralize("savedsearch") + "/" + entity
        (resp, header) = self._send("DELETE", project, None, resource, params, headers)
        return DeleteEntityResponse(header, resp)

    def update_savedsearch(self, project, detail):
        """update savedsearch
        Unsuccessful opertaion will cause an LogException.

        :type project: string
        :param project: the project name

        :type detail: dict/string
        :param detail: json string of savedsearch config details

        :return: UpdateEntityResponse

        :raise: LogException
        """
        params = {}
        headers = {}
        savedsearch_name = None
        if hasattr(detail, "to_json"):
            detail = detail.to_json()
            body_str = six.b(json.dumps(detail))
            savedsearch_name = detail.get("savedsearchName" or "name", "")
        elif isinstance(detail, six.binary_type):
            body_str = detail
        elif isinstance(detail, six.text_type):
            body_str = detail.encode("utf8")
        else:
            body_str = six.b(json.dumps(detail))
            savedsearch_name = detail.get("savedsearchName" or "name", "")

        if savedsearch_name is None:
            savedsearch_name = json.loads(body_str).get("savedsearchName", "")

        assert savedsearch_name, LogException(
            "InvalidParameter",
            'unknown savedsearch name in "{0}"'.format(detail),
        )
        headers["Content-Type"] = "application/json"
        headers["x-log-bodyrawsize"] = str(len(body_str))
        resource = "/" + pluralize("savedsearch") + "/" + savedsearch_name
        (resp, headers) = self._send("PUT", project, body_str, resource, params, headers)
        return UpdateEntityResponse(headers, resp)

    def create_savedsearch(self, project, detail):
        """create savedsearch
        Unsuccessful opertaion will cause an LogException.

        :type project: string
        :param project: the project name

        :type detail: dict/string
        :param detail: json string of savedsearch config details

        :return: CreateEntityResponse

        :raise: LogException
        """

        params = {}
        headers = {"x-log-bodyrawsize": "0", "Content-Type": "application/json"}

        if hasattr(detail, "to_json"):
            detail = detail.to_json()
            body_str = six.b(json.dumps(detail))
        elif isinstance(detail, six.binary_type):
            body_str = detail
        elif isinstance(detail, six.text_type):
            body_str = detail.encode("utf8")
        else:
            body_str = six.b(json.dumps(detail))

        resource = "/" + pluralize("savedsearch")
        (resp, header) = self._send("POST", project, body_str, resource, params, headers)
        return CreateEntityResponse(header, resp)

    def list_shipper(self, project, logstore, offset=0, size=100):
        """list shippers
        Unsuccessful opertaion will cause an LogException.

        :type project: string
        :param project: the project name

        :type logstore: string
        :param logstore: the logstore name

        :type offset: int
        :param offset: the offset of all the matched shippers

        :type size: int
        :param size: the max return shipper count, -1 means all

        :return: ListEntityResponse

        :raise: LogException
        """
        if int(size) == -1 or int(size) > MAX_LIST_PAGING_SIZE:
            return list_more(self.list_shipper, int(offset), int(size), MAX_LIST_PAGING_SIZE,
                             project, logstore)

        headers = {}
        params = {}
        params["offset"] = str(offset)
        params["size"] = str(size)
        resource = "/logstores/" + logstore + "/shipper"
        (resp, header) = self._send("GET", project, None, resource, params, headers)
        return ListEntityResponse(header, resp, resource_name="shipper", entities_key="shipper")

    def get_shipper(self, project, logstore, entity):
        """get shipper
        Unsuccessful opertaion will cause an LogException.

        :type project: string
        :param project: the project name

        :type logstore: string
        :param logstore: the logstore name

        :type entity: string
        :param entity: the shipper name

        :return: GetEntityResponse

        :raise: LogException
        """
        headers = {}
        params = {}
        resource = "/logstores/" + logstore + "/shipper/" + entity
        (resp, header) = self._send("GET", project, None, resource, params, headers)
        return GetEntityResponse(header, resp)

    def delete_shipper(self, project, logstore, entity):
        """delete shipper
        Unsuccessful opertaion will cause an LogException.

        :type project: string
        :param project: the project name

        :type logstore: string
        :param logstore: the logstore name

        :type entity: string
        :param entity: the shipper name

        :return: DeleteEntityResponse

        :raise: LogException
        """
        headers = {}
        params = {}
        resource = "/logstores/" + logstore + "/shipper/" + entity
        (resp, header) = self._send("DELETE", project, None, resource, params, headers)
        return DeleteEntityResponse(header, resp)

    def update_shipper(self, project, logstore, detail):
        """update shipper
        Unsuccessful opertaion will cause an LogException.

        :type project: string
        :param project: the project name

        :type logstore: string
        :param logstore: the logstore name

        :type detail: dict/string
        :param detail: json string of shipper config details

        :return: UpdateEntityResponse

        :raise: LogException
        """
        params = {}
        headers = {}
        shipper_name = None
        if hasattr(detail, "to_json"):
            detail = detail.to_json()
            body_str = six.b(json.dumps(detail))
            shipper_name = detail.get("shipperName" or "name", "")
        elif isinstance(detail, six.binary_type):
            body_str = detail
        elif isinstance(detail, six.text_type):
            body_str = detail.encode("utf8")
        else:
            body_str = six.b(json.dumps(detail))
            shipper_name = detail.get("shipperName" or "name", "")

        if shipper_name is None:
            shipper_name = json.loads(body_str).get("shipperName", "")

        assert shipper_name, LogException(
            "InvalidParameter",
            'unknown shipper name in "{0}"'.format(detail),
        )
        headers["Content-Type"] = "application/json"
        headers["x-log-bodyrawsize"] = str(len(body_str))
        resource = "/logstores/" + logstore + "/shipper/" + shipper_name
        (resp, headers) = self._send("PUT", project, body_str, resource, params, headers)
        return UpdateEntityResponse(headers, resp)

    def create_shipper(self, project, logstore, detail):
        """create shipper
        Unsuccessful opertaion will cause an LogException.

        :type project: string
        :param project: the project name

        :type logstore: string
        :param logstore: the logstore name

        :type detail: dict/string
        :param detail: json string of shipper config details

        :return: CreateEntityResponse

        :raise: LogException
        """
        params = {}
        headers = {"x-log-bodyrawsize": "0", "Content-Type": "application/json"}
        if hasattr(detail, "to_json"):
            detail = detail.to_json()
            body_str = six.b(json.dumps(detail))
        elif isinstance(detail, six.binary_type):
            body_str = detail
        elif isinstance(detail, six.text_type):
            body_str = detail.encode("utf8")
        else:
            body_str = six.b(json.dumps(detail))
        resource = "/logstores/" + logstore + "/shipper"
        (resp, header) = self._send("POST", project, body_str, resource, params, headers)
        return CreateEntityResponse(header, resp)

# make_lcrud_methods(LogClient, 'job', name_field='name', root_resource='/jobs', entities_key='results')
# make_lcrud_methods(LogClient, 'dashboard', name_field='dashboardName')
# make_lcrud_methods(LogClient, 'alert', name_field='name', root_resource='/jobs', entities_key='results', job_type="Alert")
# make_lcrud_methods(LogClient, 'savedsearch', name_field='savedsearchName')
# make_lcrud_methods(LogClient, 'shipper', logstore_level=True, root_resource='/shipper', name_field='shipperName', raw_resource_name='shipper')
