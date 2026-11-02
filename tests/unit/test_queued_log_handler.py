# encoding: utf-8
import atexit
import logging
import threading

from aliyun.log.logger_hanlder import QueuedLogHandler


class RecordingHandler(QueuedLogHandler):
    def __init__(self, **kwargs):
        self.sent = []
        self.errors = []
        close_wait = kwargs.pop('close_wait', 1)
        super(RecordingHandler, self).__init__(
            'localhost', 'test-id', 'test-key', 'project', 'store',
            put_wait=0.01, close_wait=close_wait, **kwargs)

    def send(self, request):
        self.sent.extend(request.get_log_items())

    def handleError(self, record):
        self.errors.append(record)


def make_record(message):
    return logging.LogRecord('test', logging.INFO, __file__, 1, message, (), None)


def cleanup(handler):
    handler.stop()
    handler.close()
    if hasattr(atexit, 'unregister'):
        atexit.unregister(handler.stop)


def test_flush_keeps_worker_available_for_later_records():
    handler = RecordingHandler(batch_size=2)
    try:
        handler.emit(make_record('first'))
        handler.emit(make_record('second'))
        handler.flush()
        assert len(handler.sent) == 2
        assert handler.worker.is_alive()
        assert not handler.stop_flag

        handler.emit(make_record('third'))
        handler.flush()
        assert len(handler.sent) == 3
    finally:
        cleanup(handler)


def test_empty_flush_does_not_stop_worker():
    handler = RecordingHandler()
    try:
        handler.flush()
        assert handler.worker.is_alive()
        assert not handler.stop_flag
    finally:
        cleanup(handler)


def test_flush_waits_for_in_flight_send():
    sending = threading.Event()
    release = threading.Event()
    flushed = threading.Event()

    class BlockingHandler(RecordingHandler):
        def send(self, request):
            sending.set()
            assert release.wait(3)
            super(BlockingHandler, self).send(request)

    handler = BlockingHandler(batch_size=1)
    flush_thread = None
    try:
        handler.emit(make_record('blocked'))
        assert sending.wait(2)

        def flush():
            handler.flush()
            flushed.set()

        flush_thread = threading.Thread(target=flush)
        flush_thread.start()
        assert not flushed.wait(0.05)
        release.set()
        assert flushed.wait(2)
        assert len(handler.sent) == 1
        assert handler.worker.is_alive()
    finally:
        release.set()
        if flush_thread is not None:
            flush_thread.join(3)
        cleanup(handler)


def test_flush_timeout_keeps_pending_send_and_worker():
    sending = threading.Event()
    release = threading.Event()
    flushed = threading.Event()

    class BlockingHandler(RecordingHandler):
        def send(self, request):
            sending.set()
            assert release.wait(3)
            super(BlockingHandler, self).send(request)

    handler = BlockingHandler(batch_size=1, close_wait=0.05)
    flush_thread = None
    try:
        handler.emit(make_record('blocked'))
        assert sending.wait(2)

        def flush():
            handler.flush()
            flushed.set()

        flush_thread = threading.Thread(target=flush)
        flush_thread.start()
        assert flushed.wait(1)
        assert handler.queue.unfinished_tasks == 1
        assert handler.worker.is_alive()
        assert not handler.stop_flag
    finally:
        release.set()
        if flush_thread is not None:
            flush_thread.join(3)
        cleanup(handler)


def test_failed_send_completes_queue_accounting():
    class FailingHandler(RecordingHandler):
        def send(self, request):
            raise RuntimeError('send failed')

    handler = FailingHandler(batch_size=1)
    try:
        handler.emit(make_record('failed'))
        handler.flush()
        assert len(handler.errors) == 1
        assert handler.queue.unfinished_tasks == 0
        assert handler.worker.is_alive()
    finally:
        cleanup(handler)


def test_close_stops_worker():
    handler = RecordingHandler()
    try:
        handler.emit(make_record('last'))
        handler.close()
        assert not handler.worker.is_alive()
        assert len(handler.sent) == 1
    finally:
        cleanup(handler)
