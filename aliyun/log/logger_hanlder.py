import logging
from .logclient import LogClient
from .logitem import LogItem
from .putlogsrequest import PutLogsRequest
from threading import Thread
import atexit
from time import time
from enum import Enum
from .version import LOGGING_HANDLER_USER_AGENT

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
    func_name = 'funcName'
    module = 'module'
    file_path = 'pathname'
    line_no = 'lineno'
    process_id = 'process'
    process_name = 'processName'
    thread_id = 'thread'
    thread_name = 'threadName'


class SimpleLogHandler(logging.Handler, object):
    """
    SimpleLogHandler, blocked sending any logs, just for simple test purpose

    :param end_point: log service endpoint

    :param access_key_id: access key id

    :param access_key: access key

    :param project: project name

    :param log_store: logstore name

    :param topic: topic, by default is empty

    :param fields: list of LogFields or list of names of LogFields, default is LogFields.record_name, LogFields.level, LogFields.func_name, LogFields.module, LogFields.file_path, LogFields.line_no, LogFields.process_id, LogFields.process_name, LogFields.thread_id, LogFields.thread_name

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

    :param kwargs: other parameters  passed to logging.Handler
    """

    def __init__(self, end_point, access_key_id, access_key, project, log_store, topic=None, fields=None,
                 buildin_fields_prefix=None, buildin_fields_suffix=None,
                 extract_json=None, extract_json_drop_message=None,
                 extract_json_prefix=None, extract_json_suffix=None,
                 extract_kv=None, extract_kv_drop_message=None,
                 extract_kv_prefix=None, extract_kv_suffix=None,
                 extract_kv_sep=None,
                 **kwargs):
        logging.Handler.__init__(self, **kwargs)
        self.end_point = end_point
        self.access_key_id = access_key_id
        self.access_key = access_key
        self.project = project
        self.log_store = log_store
        self.client = None
        self.topic = topic
        self.fields = (LogFields.record_name, LogFields.level,
                       LogFields.func_name, LogFields.module,
                       LogFields.file_path, LogFields.line_no,
                       LogFields.process_id, LogFields.process_name,
                       LogFields.thread_id, LogFields.thread_name) if fields is None else fields

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

    def set_topic(self, topic):
        self.topic = topic

    def create_client(self):
        self.client = LogClient(self.end_point, self.access_key_id, self.access_key)
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

        if isinstance(v, (dict, list)):
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

    def make_request(self, record):
        contents = []
        message_field_name = "{0}message{1}".format(self.buildin_fields_prefix, self.buildin_fields_suffix)
        if isinstance(record.msg, dict) and self.extract_json:
            data = self.extract_dict(record.msg)
            contents.extend(data)

            if not self.extract_json_drop_message or not data:
                contents.append((message_field_name, self.format(record)))
        elif isinstance(record.msg, (six.text_type, six.binary_type)) and self.extract_kv:
            data = self.extract_kv_str(record.msg)
            contents.extend(data)

            if not self.extract_kv_drop_message or not data:  # if it's not KV
                contents.append((message_field_name, self.format(record)))
        else:
            contents = [(message_field_name, self.format(record))]

        # add builtin fields
        for x in self.fields:
            if isinstance(x, (six.binary_type, six.text_type)):
                x = LogFields[x]

            v = getattr(record, x.value)
            if not isinstance(v, (six.binary_type, six.text_type)):
                v = str(v)
            contents.append(("{0}{1}{2}".format(self.buildin_fields_prefix, x.name, self.buildin_fields_suffix), v))

        item = LogItem(contents=contents, timestamp=record.created)

        return PutLogsRequest(self.project, self.log_store, self.topic, logitems=[item, ])

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

    :param queue_size: queue size, default is 4096 logs

    :param put_wait: maximum delay to send the logs, by default 2 seconds

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
                                               **kwargs)
        self.stop_flag = False
        self.stop_time = None
        self.put_wait = put_wait or 2  # default is 2 seconds
        self.close_wait = close_wait or 5  # default is 5 seconds
        self.queue_size = queue_size or 4096  # default is 4096 items
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
            self.queue.put(req, timeout=self.put_wait)
        except Full as ex:
            self.handleError(record)

    def _get_batch_requests(self, timeout=None):
        reqs = []
        s = time()
        while len(reqs) < self.batch_size:
            try:
                req = self.queue.get(timeout=timeout)
                self.queue.task_done()

                reqs.append(req)

                if (time() - s) >= timeout:
                    break
            except Empty as ex:
                break

        if not reqs:
            raise Empty
        elif len(reqs) <= 1:
            return reqs[0]
        else:
            logitems = []
            req = reqs[0]
            for req in reqs:
                logitems.extend(req.get_log_items())

            ret = PutLogsRequest(self.project, self.log_store, req.topic, logitems=logitems)
            ret.__record__ = req.__record__

            return ret

    def _post(self):
        while not self.stop_flag or (time() - self.stop_time) <= self.close_wait:
            try:
                req = self._get_batch_requests(timeout=2)
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

    :param queue_size: queue size, default is 4096 logs

    :param put_wait: maximum delay to send the logs, by default 2 seconds

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

    :param kwargs: other parameters  passed to logging.Handler
    """
    def __init__(self, *args, **kwargs):
        super(UwsgiQueuedLogHandler, self).__init__(*args, **kwargs)

    def init_worker(self):
        self.queue = Queue(self.queue_size)

        from uwsgidecorators import postfork, thread
        self._post = postfork(thread(self._post))

    def stop(self):
        self.stop_time = time()
        self.stop_flag = True

