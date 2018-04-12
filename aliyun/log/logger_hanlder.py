import logging
from .logclient import LogClient
from .logitem import LogItem
from .putlogsrequest import PutLogsRequest
from threading import Thread
import atexit
from time import time
from enum import Enum
import six
if six.PY2:
    from Queue import Empty, Full, Queue
else:
    from queue import Empty, Full, Queue


class LogFields(Enum):
    record_name = 'name'
    level = 'levelname'
    func_name = 'funcName'
    module = 'module'
    file_path  = 'pathname'
    line_no = 'lineno'
    process_id = 'process'
    process_name = 'processName'
    thread_id = 'thread'
    thread_name = 'threadName'


class SimpleLogHandler(logging.Handler, object):
    def __init__(self, end_point, access_key_id, access_key, project, log_store, **kwargs):
        logging.Handler.__init__(self, **kwargs)
        self.end_point = end_point
        self.access_key_id = access_key_id
        self.access_key = access_key
        self.project = project
        self.log_store = log_store
        self.client = None
        self.topic = None
        self.fields = (LogFields.record_name, LogFields.level,
                       LogFields.func_name, LogFields.module,
                       LogFields.file_path, LogFields.line_no,
                       LogFields.process_id, LogFields.process_name,
                       LogFields.thread_id, LogFields.thread_name)

    def set_topic(self, topic):
        self.topic = topic

    def create_client(self):
        self.client = LogClient(self.end_point, self.access_key_id, self.access_key)

    def send(self, req):
        if self.client is None:
            self.create_client()
        return self.client.put_logs(req)

    def set_fields(self, fields):
        self.fields = fields

    def make_request(self, record):
        contents = [('message', self.format(record))]
        for x in self.fields:
            v = getattr(record, x.value)
            if not isinstance(v, (six.binary_type, six.text_type)):
                v = str(v)
            contents.append((x.name, v))

        item = LogItem(contents=contents, timestamp=record.created)

        return PutLogsRequest(self.project, self.log_store, self.topic, logitems=[item, ])

    def emit(self, record):
        try:
            req = self.make_request(record)
            self.send(req)
        except Exception as e:
            self.handleError(record)


class QueuedLogHandler(SimpleLogHandler):
    def __init__(self, end_point, access_key_id, access_key, project, log_store, queue_size=None, put_wait=None, close_wait=None, batch_size=None, **kwargs):
        super(QueuedLogHandler, self).__init__(end_point, access_key_id, access_key, project, log_store, **kwargs)
        self.stop_flag = False
        self.stop_time = None
        self.put_wait = put_wait or 2                           # default is 2 seconds
        self.close_wait = close_wait or 5                       # default is 5 seconds
        self.queue_size = queue_size or 4096                    # default is 4096 items
        self.batch_size = min(batch_size or 1024, self.queue_size)   # default is 1024 items

        self.worker = Thread(target=self._post)
        self.queue = Queue(self.queue_size)
        self.worker.setDaemon(True)

        self.worker.start()
        atexit.register(self.stop)

    def stop(self):
        self.stop_time = time()
        self.stop_flag = True
        self.worker.join()

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
