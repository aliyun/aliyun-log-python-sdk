
import logging
from .logclient import LogClient
from .logitem import LogItem
from .putlogsrequest import PutLogsRequest


class AliyunLogHandler(logging.Handler):

    def __init__(self, end_point, access_key_id, access_key, project, log_store, **kwargs):
        logging.Handler.__init__(self, **kwargs)
        self.end_point = end_point
        self.access_key_id = access_key_id
        self.access_key = access_key
        self.project = project
        self.log_store = log_store
        self.client = None

    def create_client(self):
        self.client = LogClient(self.end_point, self.access_key_id, self.access_key)

    def send(self, req):
        if self.client is None:
            self.create_client()
        return self.client.put_logs(req)

    def make_request(self, record):
        contents = [('message', self.format(record)),
                    ('level', record.levelname),
                    ('func_name', record.funcName),
                    ('path_name', record.pathname),
                    ('line_no', str(record.lineno))]

        item = LogItem(contents=contents)
        return PutLogsRequest(self.project, self.log_store, record.name, logitems=[item, ])

    def emit(self, record):
        try:
            req = self.make_request(record)
            self.send(req)
        except Exception as e:
            self.handleError(record)
