import logging
from .logclient import LogClient
from .logitem import LogItem
from .putlogsrequest import PutLogsRequest
from threading import Thread
import atexit
from time import time, sleep
from enum import Enum
from .version import LOGGING_HANDLER_USER_AGENT
from .auth import *
import six
try:
    from collections.abc import Callable
except ImportError:
    from collections import Callable
import six

if six.PY2:
    from Queue import Empty, Full, Queue
else:
    from queue import Empty, Full, Queue

import json
import re


class LogFields(Enum):
    """fields used to upload automatically
    Possible fields:
    record_name, level, func_name, module,
    file_path, line_no, process_id,
    process_name, thread_id, thread_name
    """
    record_name = 'name'
    level = 'levelname'
    file_name = 'filename'
    func_name = 'funcName'
    module = 'module'
    file_path = 'pathname'
    line_no = 'lineno'
    process_id = 'process'
    process_name = 'processName'
    thread_id = 'thread'
    thread_name = 'threadName'

    level_no = 'levelno'
    asc_time = 'asctime'
    created_timestamp = 'created'
    micro_second = 'msecs'
    relative_created = 'relativeCreated'


DEFAULT_RECORD_LOG_FIELDS = set((LogFields.record_name, LogFields.level,
                                 LogFields.func_name, LogFields.module,
                                 LogFields.file_path, LogFields.line_no,
                                 LogFields.process_id, LogFields.process_name,
                                 LogFields.thread_id, LogFields.thread_name))

BLACK_FIELD_LIST = set(['exc_info', 'exc_text', 'stack_info', 'msg', 'args', 'message'])

BUILTIN_LOG_FIELDS_NAMES = set(x for x in dir(LogFields) if not x.startswith('__'))
BUILTIN_LOG_FIELDS_NAMES.update(set(LogFields[x].value for x in BUILTIN_LOG_FIELDS_NAMES))
BUILTIN_LOG_FIELDS_NAMES.update(BLACK_FIELD_LIST)


class SimpleLogHandler(logging.Handler, object):
    """
    SimpleLogHandler, blocked sending any logs, just for simple test purpose

    :param end_point: log service endpoint

    :param access_key_id: access key id

    :param access_key: access key

    :param project: project name

    :param log_store: logstore name

    :param topic: topic, by default is empty

    :param fields: list of LogFields or list of names of LogFields, default is LogFields.record_name, LogFields.level, LogFields.func_name, LogFields.module, LogFields.file_path, LogFields.line_no, LogFields.process_id, LogFields.process_name, LogFields.thread_id, LogFields.thread_name, you could also just use he string name like 'thread_name', it's also possible customize extra fields in this list by disable extra fields and put white list here.

    :param buildin_fields_prefix: prefix of builtin fields, default is empty. suggest using "__" when extract json is True to prevent conflict.

    :param buildin_fields_suffix: suffix of builtin fields, default is empty. suggest using "__" when extract json is True to prevent conflict.

    :param extract_json: if extract json automatically, default is False

    :param extract_json_drop_message: if drop message fields if it's JSON and extract_json is True, default is False

    :param extract_json_prefix: prefix of fields extracted from json when extract_json is True. default is ""

    :param extract_json_suffix: suffix of fields extracted from json when extract_json is True. default is empty

    :param extract_kv: if extract kv like k1=v1 k2="v 2" automatically, default is False

    :param extract_kv_drop_message: if drop message fields if it's kv and extract_kv is True, default is False

    :param extract_kv_prefix: prefix of fields extracted from KV when extract_json is True. default is ""

    :param extract_kv_suffix: suffix of fields extracted from KV when extract_json is True. default is ""

    :param extract_kv_sep: separator for KV case, defualt is '=', e.g. k1=v1

    :param extra: if show extra info, default True to show all. default is True. Note: the extra field will also be handled with buildin_fields_prefix/suffix

    :param auth_version: only support AUTH_VERSION_1 and AUTH_VERSION_4 currently

    :param region: the region of project

    :param kwargs: other parameters  passed to logging.Handler
    """

    def __init__(self, end_point, access_key_id, access_key, project, log_store, topic=None, fields=None,
                 buildin_fields_prefix=None, buildin_fields_suffix=None,
                 extract_json=None, extract_json_drop_message=None,
                 extract_json_prefix=None, extract_json_suffix=None,
                 extract_kv=None, extract_kv_drop_message=None,
                 extract_kv_prefix=None, extract_kv_suffix=None,
                 extract_kv_sep=None, extra=None,
                 auth_version=AUTH_VERSION_1, region='',
                 **kwargs):
        logging.Handler.__init__(self, **kwargs)
        self.end_point = end_point
        self.access_key_id = access_key_id
        self.access_key = access_key
        self.project = project
        self.log_store = log_store
        self.client = None
        self.topic = topic
        self.fields = DEFAULT_RECORD_LOG_FIELDS if fields is None else set(fields)
        self.auth_version = auth_version
        self.region = region

        self.extract_json = False if extract_json is None else extract_json
        self.extract_json_prefix = "" if extract_json_prefix is None else extract_json_prefix
        self.extract_json_suffix = "" if extract_json_suffix is None else extract_json_suffix
        self.extract_json_drop_message = False if extract_json_drop_message is None else extract_json_drop_message
        self.buildin_fields_prefix = "" if buildin_fields_prefix is None else buildin_fields_prefix
        self.buildin_fields_suffix = "" if buildin_fields_suffix is None else buildin_fields_suffix

        self.extract_kv = False if extract_kv is None else extract_kv
        self.extract_kv_prefix = "" if extract_kv_prefix is None else extract_kv_prefix
        self.extract_kv_suffix = "" if extract_kv_suffix is None else extract_kv_suffix
        self.extract_kv_drop_message = False if extract_kv_drop_message is None else extract_kv_drop_message
        self.extract_kv_sep = "=" if extract_kv_sep is None else extract_kv_sep
        self.extract_kv_ptn = self._get_extract_kv_ptn()
        self.extra = True if extra is None else extra
        self._skip_message = False
        self._built_in_root_filed = ""
        self._log_tags = None
        self._source = None

    @property
    def skip_message(self):
        return self._skip_message

    @skip_message.setter
    def skip_message(self, value):
        self._skip_message = value

    @property
    def built_in_root_field(self):
        return self._built_in_root_filed

    @built_in_root_field.setter
    def built_in_root_field(self, value):
        self._built_in_root_filed = value

    @property
    def log_tags(self):
        return self._log_tags

    @log_tags.setter
    def log_tags(self, value):
        self._log_tags = value

    def set_topic(self, topic):
        self.topic = topic

    @property
    def source(self):
        return self._source

    @source.setter
    def source(self, value):
        self._source = value

    def create_client(self):
        self.client = LogClient(self.end_point, self.access_key_id, self.access_key, auth_version=self.auth_version, region=self.region)
        self.client.set_user_agent(LOGGING_HANDLER_USER_AGENT)

    def send(self, req):
        if self.client is None:
            self.create_client()
        return self.client.put_logs(req)

    def set_fields(self, fields):
        self.fields = fields

    @staticmethod
    def _n(v):
        if v is None:
            return ""

        if isinstance(v, (dict, list, tuple)):
            try:
                v = json.dumps(v)
            except Exception:
                pass
        elif six.PY2 and isinstance(v, six.text_type):
            v = v.encode('utf8', "ignore")
        elif six.PY3 and isinstance(v, six.binary_type):
            v = v.decode('utf8', "ignore")

        return str(v)

    def extract_dict(self, message):
        data = []
        if isinstance(message, dict):
            for k, v in six.iteritems(message):
                data.append(("{0}{1}{2}".format(self.extract_json_prefix, self._n(k),
                                                self.extract_json_suffix), self._n(v)))
        return data

    def _get_extract_kv_ptn(self):
        sep = self.extract_kv_sep
        p1 = u'(?!{0})([\u4e00-\u9fa5\u0800-\u4e00\\w\\.\\-]+)\\s*{0}\\s*([\u4e00-\u9fa5\u0800-\u4e00\\w\\.\\-]+)'
        p2 = u'(?!{0})([\u4e00-\u9fa5\u0800-\u4e00\\w\\.\\-]+)\\s*{0}\\s*"\s*([^"]+?)\s*"'
        ps = '|'.join([p1, p2]).format(sep)

        return re.compile(ps)

    def extract_kv_str(self, message):

        if isinstance(message, six.binary_type):
            message = message.decode('utf8', 'ignore')

        r = self.extract_kv_ptn.findall(message)

        data = []
        for k1, v1, k2, v2 in r:
            if k1:
                data.append(("{0}{1}{2}".format(self.extract_kv_prefix, self._n(k1),
                                                self.extract_kv_suffix), self._n(v1)))
            elif k2:
                data.append(("{0}{1}{2}".format(self.extract_kv_prefix, self._n(k2),
                                                self.extract_kv_suffix), self._n(v2)))

        return data

    def _add_record_fields(self, record, k, contents):
        data = self._get_record_fields(record, k)
        if data:
            contents.append(data)

    def _get_record_fields(self, record, k):
        v = getattr(record, k, None)
        if v is None or isinstance(v, Callable):
            return

        v = self._n(v)
        return "{0}{1}{2}".format(self.buildin_fields_prefix, k, self.buildin_fields_suffix), v

    def make_request(self, record):
        # add builtin fields
        built_root = {}
        contents = []
        message_field_name = "{0}message{1}".format(self.buildin_fields_prefix, self.buildin_fields_suffix)
        if isinstance(record.msg, dict) and self.extract_json:
            data = self.extract_dict(record.msg)
            contents.extend(data)

            if not self.extract_json_drop_message or not data:
                if not self._skip_message:
                    if self._built_in_root_filed:
                        built_root[message_field_name] = self.format(record)
                    else:
                        contents.append((message_field_name, self.format(record)))
        elif isinstance(record.msg, (six.text_type, six.binary_type)) and self.extract_kv:
            data = self.extract_kv_str(record.msg)
            contents.extend(data)

            if not self.extract_kv_drop_message or not data:  # if it's not KV
                if not self._skip_message:
                    if self._built_in_root_filed:
                        built_root[message_field_name] = self.format(record)
                    else:
                        contents.append((message_field_name, self.format(record)))
        elif not self._skip_message:
            if self._built_in_root_filed:
                built_root[message_field_name] = self.format(record)
            else:
                contents = [(message_field_name, self.format(record))]

        for x in self.fields:
            k = x
            if isinstance(x, LogFields):
                k = x.name
                x = x.value
            elif isinstance(x, (six.binary_type, six.text_type)):
                if x in BLACK_FIELD_LIST:
                    continue  # by pass for those reserved fields. make no sense to render them

                if x in BUILTIN_LOG_FIELDS_NAMES:
                    k = LogFields[x].name
                    x = LogFields[x].value
                elif self.extra:  # will handle it later
                    continue
            if not self._built_in_root_filed:
                self._add_record_fields(record, x, contents)
            else:
                data = self._get_record_fields(record, x)
                if data:
                    built_root[data[0]] = data[1]

        if self._built_in_root_filed and built_root:
            contents.append((self._n(self._built_in_root_filed), json.dumps(built_root)))

        # handle extra
        if self.extra:
            for x in dir(record):
                if not x.startswith('__') and not x in BUILTIN_LOG_FIELDS_NAMES:
                    self._add_record_fields(record, x, contents)

        item = LogItem(contents=contents, timestamp=record.created)

        return PutLogsRequest(self.project, self.log_store, self.topic, source=self._source, logitems=[item, ], logtags=self._log_tags)

    def emit(self, record):
        try:
            req = self.make_request(record)
            self.send(req)
        except Exception as e:
            self.handleError(record)


class QueuedLogHandler(SimpleLogHandler):
    """
    Queued Log Handler, tuned async log handler.

    :param end_point: log service endpoint

    :param access_key_id: access key id

    :param access_key: access key

    :param project: project name

    :param log_store: logstore name

    :param topic: topic, default is empty

    :param fields: list of LogFields, default is LogFields.record_name, LogFields.level, LogFields.func_name, LogFields.module, LogFields.file_path, LogFields.line_no, LogFields.process_id, LogFields.process_name, LogFields.thread_id, LogFields.thread_name

    :param queue_size: queue size, default is 40960 logs, about 10MB ~ 40MB

    :param put_wait: maximum delay to send the logs, by default 2 seconds and wait double time for when Queue is full.

    :param close_wait: when program exit, it will try to send all logs in queue in this timeperiod, by default 5 seconds

    :param batch_size: merge this cound of logs and send them batch, by default min(1024, queue_size)

    :param buildin_fields_prefix: prefix of builtin fields, default is empty. suggest using "__" when extract json is True to prevent conflict.

    :param buildin_fields_suffix: suffix of builtin fields, default is empty. suggest using "__" when extract json is True to prevent conflict.

    :param extract_json: if extract json automatically, default is False

    :param extract_json_drop_message: if drop message fields if it's JSON and extract_json is True, default is False

    :param extract_json_prefix: prefix of fields extracted from json when extract_json is True. default is ""

    :param extract_json_suffix: suffix of fields extracted from json when extract_json is True. default is empty

    :param extract_kv: if extract kv like k1=v1 k2="v 2" automatically, default is False

    :param extract_kv_drop_message: if drop message fields if it's kv and extract_kv is True, default is False

    :param extract_kv_prefix: prefix of fields extracted from KV when extract_json is True. default is ""

    :param extract_kv_suffix: suffix of fields extracted from KV when extract_json is True. default is ""

    :param extract_kv_sep: separator for KV case, defualt is '=', e.g. k1=v1

    :param extra: if show extra info, default True to show all. default is True

    :param auth_version: only support AUTH_VERSION_1 and AUTH_VERSION_4 currently

    :param region: the region of project

    :param kwargs: other parameters  passed to logging.Handler
    """

    def __init__(self, end_point, access_key_id, access_key, project, log_store, topic=None, fields=None,
                 queue_size=None, put_wait=None, close_wait=None, batch_size=None,
                 buildin_fields_prefix=None, buildin_fields_suffix=None,
                 extract_json=None, extract_json_drop_message=None,
                 extract_json_prefix=None, extract_json_suffix=None,
                 extract_kv=None, extract_kv_drop_message=None,
                 extract_kv_prefix=None, extract_kv_suffix=None,
                 extract_kv_sep=None,
                 extra=None,
                 auth_version=AUTH_VERSION_1, region='',
                 **kwargs):
        super(QueuedLogHandler, self).__init__(end_point, access_key_id, access_key, project, log_store,
                                               topic=topic, fields=fields,
                                               extract_json=extract_json,
                                               extract_json_drop_message=extract_json_drop_message,
                                               extract_json_prefix=extract_json_prefix,
                                               extract_json_suffix=extract_json_suffix,
                                               buildin_fields_prefix=buildin_fields_prefix,
                                               buildin_fields_suffix=buildin_fields_suffix,
                                               extract_kv=extract_kv,
                                               extract_kv_drop_message=extract_kv_drop_message,
                                               extract_kv_prefix=extract_kv_prefix,
                                               extract_kv_suffix=extract_kv_suffix,
                                               extract_kv_sep=extract_kv_sep,
                                               extra=extra,
                                               auth_version=auth_version, region=region,
                                               **kwargs)
        self.stop_flag = False
        self.stop_time = None
        self.put_wait = put_wait or 2  # default is 2 seconds
        self.close_wait = close_wait or 5  # default is 5 seconds
        self.queue_size = queue_size or 40960  # default is 40960, about 10MB ~ 40MB
        self.batch_size = min(batch_size or 1024, self.queue_size)  # default is 1024 items

        self.init_worker()

    def init_worker(self):
        self.worker = Thread(target=self._post)
        self.queue = Queue(self.queue_size)
        self.worker.setDaemon(True)

        self.worker.start()
        atexit.register(self.stop)

    def flush(self):
        self.stop()

    def stop(self):
        self.stop_time = time()
        self.stop_flag = True
        self.worker.join(timeout=self.close_wait + 1)

    def emit(self, record):
        req = self.make_request(record)
        req.__record__ = record
        try:
            self.queue.put(req, timeout=self.put_wait*2)
        except Full as ex:
            self.handleError(record)

    def _get_batch_requests(self, timeout=None):
        """try to get request as fast as possible, once empty and stop flag or time-out, just return Empty"""
        reqs = []
        s = time()
        while len(reqs) < self.batch_size and (time() - s) < timeout:
            try:
                req = self.queue.get(block=False)
                self.queue.task_done()

                reqs.append(req)
            except Empty as ex:
                if self.stop_flag:
                    break
                else:
                    sleep(0.1)

        if not reqs:
            raise Empty
        elif len(reqs) <= 1:
            return reqs[0]
        else:
            logitems = []
            req = reqs[0]
            for req in reqs:
                logitems.extend(req.get_log_items())

            ret = PutLogsRequest(self.project, self.log_store, req.topic, logitems=logitems, logtags=self._log_tags)
            ret.__record__ = req.__record__

            return ret

    def _post(self):
        while not self.stop_flag or (time() - self.stop_time) <= self.close_wait:
            try:
                req = self._get_batch_requests(timeout=self.put_wait)
            except Empty as ex:
                if self.stop_flag:
                    break
                else:
                    continue

            try:
                self.send(req)
            except Exception as ex:
                self.handleError(req.__record__)


class UwsgiQueuedLogHandler(QueuedLogHandler):
    """
    Queued Log Handler for Uwsgi, depends on library `uwsgidecorators`, need to deploy it separatedly.

    :param end_point: log service endpoint

    :param access_key_id: access key id

    :param access_key: access key

    :param project: project name

    :param log_store: logstore name

    :param topic: topic, default is empty

    :param fields: list of LogFields, default is LogFields.record_name, LogFields.level, LogFields.func_name, LogFields.module, LogFields.file_path, LogFields.line_no, LogFields.process_id, LogFields.process_name, LogFields.thread_id, LogFields.thread_name

    :param queue_size: queue size, default is 40960 logs, about 10MB ~ 40MB

    :param put_wait: maximum delay to send the logs, by default 2 seconds and wait double time for when Queue is full.

    :param close_wait: when program exit, it will try to send all logs in queue in this timeperiod, by default 2 seconds

    :param batch_size: merge this cound of logs and send them batch, by default min(1024, queue_size)

    :param buildin_fields_prefix: prefix of builtin fields, default is empty. suggest using "__" when extract json is True to prevent conflict.

    :param buildin_fields_suffix: suffix of builtin fields, default is empty. suggest using "__" when extract json is True to prevent conflict.

    :param extract_json: if extract json automatically, default is False

    :param extract_json_drop_message: if drop message fields if it's JSON and extract_json is True, default is False

    :param extract_json_prefix: prefix of fields extracted from json when extract_json is True. default is ""

    :param extract_json_suffix: suffix of fields extracted from json when extract_json is True. default is empty

    :param extract_kv: if extract kv like k1=v1 k2="v 2" automatically, default is False

    :param extract_kv_drop_message: if drop message fields if it's kv and extract_kv is True, default is False

    :param extract_kv_prefix: prefix of fields extracted from KV when extract_json is True. default is ""

    :param extract_kv_suffix: suffix of fields extracted from KV when extract_json is True. default is ""

    :param extract_kv_sep: separator for KV case, defualt is '=', e.g. k1=v1

    :param extra: if show extra info, default True to show all. default is True

    :param auth_version: only support AUTH_VERSION_1 and AUTH_VERSION_4 currently

    :param region: the region of project

    :param kwargs: other parameters  passed to logging.Handler
    """
    def __init__(self, *args, **kwargs):
        # change close_wait from default 5 to 2
        if len(args) >= 10:
            if args[9] is None:
                args = args[:9] + (2,) + args[10:]
        elif 'close_wait' in kwargs and kwargs['close_wait'] is None:
            kwargs['close_wait'] = 2

        super(UwsgiQueuedLogHandler, self).__init__(*args, **kwargs)

    def init_worker(self):
        self.queue = Queue(self.queue_size)

        from uwsgidecorators import postfork, thread
        self._post = postfork(thread(self._post))

    def stop(self):
        self.stop_time = time()
        self.stop_flag = True
