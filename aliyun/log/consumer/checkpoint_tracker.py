# -*- coding: utf-8 -*-

import time

from .exceptions import CheckPointException
from ..logexception import LogException
from threading import RLock, Thread, Event
from ..util import PrefixLoggerAdapter
import logging
import json
from datetime import timedelta


class PeriodicJob(Thread):
    def __init__(self, interval, worker, *args, **kwargs):
        super(PeriodicJob, self).__init__()
        self.daemon = True
        self.stopped = Event()
        self.interval = timedelta(seconds=interval)
        self.worker = worker
        self.args, self.kwargs = args, kwargs

    def stop(self):
        self.stopped.set()

    def run(self):
        while not self.stopped.wait(self.interval.total_seconds()):
            self.worker(*self.args, **self.kwargs)


def update_metering_state(
        metering_state, start, end, r_flow, r_pub_net,
        r_lines, w_flow, w_pub_net, w_lines,
):
    metering_state["start"] = start
    metering_state["end"] = end
    metering_state["r_flow"] += r_flow
    metering_state["r_pub_net"] += r_pub_net
    metering_state["r_lines"] += r_lines
    metering_state["w_flow"] += w_flow
    metering_state["w_pub_net"] += w_pub_net
    metering_state["w_lines"] += w_lines


class ConsumerCheckpointTracker(object):

    def __init__(self, loghub_client_adapter, consumer_name, shard_id, cursor_end_time=None, shard_worker=None):
        self.consumer_group_client = loghub_client_adapter
        ckpt = self.consumer_group_client.get_check_point(shard_id)
        self.consumer_name = consumer_name
        self.shard_id = shard_id
        self.cursor_end_time = cursor_end_time
        self.cursor = ckpt.get('checkpoint', '')
        self.temp_check_point = ''
        self.set_checkpoint_time = time.time()
        self.last_cursor_time = ckpt.get('updateTime', 0) / 10**6
        if self.last_cursor_time < 1:
            # Use current time as starting-point for initializing tasks
            # Not use task beginning time because it may be 1970-01-01 00:00:00
            self.last_cursor_time = time.time()
        self.last_persistent_checkpoint = ''
        self.default_flush_check_point_interval = 60
        self.default_flush_check_metric_interval = loghub_client_adapter.flush_check_metric_interval or 300
        self.shard_worker = shard_worker

        self._progress_state = {
            'accept': 0,
            'dropped': 0,
            'delivered': 0,
            'failed': 0,
        }
        self._metric_info_state = {
            'start': 0,
            'end': 0,
            'r_flow': 0,
            'r_pub_net': 0,
            'r_lines': 0,
            'w_flow': 0,
            'w_pub_net': 0,
            'w_lines': 0,
        }
        self._metric_info_lock = RLock()

        self.lock = RLock()
        log_prefix = '/'.join([loghub_client_adapter.mproject, loghub_client_adapter.mlogstore,
                               loghub_client_adapter.mconsumer_group, loghub_client_adapter.mconsumer, str(shard_id)])
        extra = {
            "etl_context": """{
            "project": "%s", 
            "logstore": "%s", 
            "consumer_group": "%s", 
            "consumer": "%s",
            "shard_id": "%s" }""" % (loghub_client_adapter.mproject,
                                         loghub_client_adapter.mlogstore,
                                         loghub_client_adapter.mconsumer_group,
                                         loghub_client_adapter.mconsumer,
                                        shard_id)
        }
        self.logger = PrefixLoggerAdapter("[{0}]".format(log_prefix), extra, logging.getLogger(__name__), {})
        metric_extra = {}
        self.metric_fields_from_scheduler = loghub_client_adapter.metric_fields_from_scheduler
        if self.metric_fields_from_scheduler is not None:
            metric_extra = {
                "type": "T",
                "uid": self.metric_fields_from_scheduler.get("uid"),
                "project": loghub_client_adapter.mproject,
                "project_id": self.metric_fields_from_scheduler.get("project_id"),
                "job": self.metric_fields_from_scheduler.get("job"),
                "s_id": self.metric_fields_from_scheduler.get("s_id"),
                "ins_type": self.metric_fields_from_scheduler.get("ins_type"),
                "src": "SLS",
                "sink": "SLS",
                "v": "1.0",
                "src_ins": loghub_client_adapter.mproject + "," + loghub_client_adapter.mlogstore,
            }

        self.metric_logger = PrefixLoggerAdapter("[{0}]".format(log_prefix), metric_extra, logging.getLogger("metric_log"), {})

        self._refresher_ckpt = PeriodicJob(
            self.default_flush_check_point_interval,
            self.flush_check_point,
        )
        self._refresher_ckpt.start()

        self._refresher_metric = PeriodicJob(
            self.default_flush_check_metric_interval,
            self.flush_check_metric,
        )
        self._refresher_metric.start()
        self.last_no_data_time = int(time.time()) - 1
        self.last_no_data_time_inited = False

    def update_progress_state(self, accept, dropped, delivered, failed):
        with self.lock:
            self._progress_state['accept'] += accept
            self._progress_state['dropped'] += dropped
            self._progress_state['delivered'] += delivered
            self._progress_state['failed'] += failed

    def _reset_progress_state(self):
        for k, v in self._progress_state.items():
            self._progress_state[k] = 0

    def update_metric_info_state(
            self, start, start_new, end, r_flow, r_pub_net,
            r_lines, w_flow, w_pub_net, w_lines,
    ):
        with self._metric_info_lock:
            if self._metric_info_state["start"] == 0:
                start = start_new
            update_metering_state(
                self._metric_info_state, start, end,
                r_flow, r_pub_net, r_lines, w_flow, w_pub_net, w_lines,
            )
            return start

    def _reset_metric_info_state(self):
        for k, v in self._metric_info_state.items():
            self._metric_info_state[k] = 0

    def set_cursor(self, cursor):
        self.cursor = cursor

    def get_cursor(self):
        return self.cursor

    def save_check_point(self, persistent, cursor=None):
        if cursor is not None:
            self.temp_check_point = cursor
        else:
            self.temp_check_point = self.cursor
        if persistent:
            self.flush_check_point()

    def set_memory_check_point(self, cursor):
        self.temp_check_point = cursor

    def set_persistent_check_point(self, cursor):
        with self.lock:
            self.last_persistent_checkpoint = cursor

    def _persistent_checkpoint(self):
        # temp value for faster performance
        tmp_checkpoint = self.temp_check_point
        cursor_time = None
        is_cpt_changed = False
        if tmp_checkpoint not in ('', self.last_persistent_checkpoint):
            try:
                cursor_time = self.consumer_group_client.update_check_point(
                    self.shard_id, self.consumer_name, tmp_checkpoint)
                self.last_persistent_checkpoint = tmp_checkpoint
                self.set_checkpoint_time = time.time()
                is_cpt_changed = True
            except LogException as e:
                msg = "fail to save check point at %s, error: %s" % (tmp_checkpoint, e)
                self.logger.error(msg, extra={"event_id": "shard_worker:metrics:checkpoint", "reason": msg})
                raise CheckPointException(
                    "Failed to persistent the cursor to outside system, " +
                    self.consumer_name + ", " + str(self.shard_id)
                    + ", " + tmp_checkpoint, e)
        return tmp_checkpoint, cursor_time, is_cpt_changed

    def flush_check_point(self):
        with self.lock:
            start_time = time.time()
            tmp_checkpoint, cursor_time, is_cpt_changed = self._persistent_checkpoint()
            state = json.dumps(self._progress_state)

            # reset state
            self._reset_progress_state()

            if cursor_time:
                self.last_cursor_time = cursor_time
            else:
                cursor_time = self.last_cursor_time

            if self.cursor_end_time is not None and self.cursor_end_time > 0:
                latency = self.cursor_end_time - cursor_time
                latency = 0 if latency < 1e-3 else latency
            else:
                latency = abs(cursor_time - start_time)
                if not self.last_no_data_time_inited and is_cpt_changed:
                    # when shard migrate (hpa|re-balance|failover)
                    self.last_no_data_time = cursor_time
                    self.last_no_data_time_inited = True
                if not self.shard_worker.is_processing_data() and not is_cpt_changed:
                    # when no data is in processing , force latency to 0
                    latency = 0
                    self.last_no_data_time = int(time.time())
                if not is_cpt_changed:
                    # for sparse data input, reset latency
                    no_data_latency = int(time.time()) - self.last_no_data_time
                    if latency > no_data_latency:
                        # latency should never large than no_data_latency
                        latency = no_data_latency

            self.logger.info(
                "check point is saved at: %s, cursor time: %s, set checkpint time: %s, latency: %s, progress: %s",
                tmp_checkpoint,
                cursor_time, self.set_checkpoint_time, latency,
                state,
                extra={"event_id": "shard_worker:metrics:checkpoint",
                       "extra_info_params": """{"checkpoint": "%s", "cursor_time": "%s", "set_checkpoint_time": "%s", "latency_second": "%s" }""" % (
                       tmp_checkpoint, cursor_time, self.set_checkpoint_time,
                       latency),
                       "progress": state})

    def flush_check_metric(self):
        # metric log
        with self._metric_info_lock:
            if self.metric_fields_from_scheduler is not None:
                metric_state = self._metric_info_state
                if metric_state:
                    metric_state = {k: str(v) for k, v in metric_state.items()}
                    metric_state['time'] = str(int(time.time()))
                    if int(metric_state['r_lines']) > 0:
                        self.metric_logger.info("", extra=metric_state)
                # reset metric_info_state
                self._reset_metric_info_state()

    def get_check_point(self):
        return self.temp_check_point

    def stop_refresher(self):
        self.logger.info('stop periodic refresher')
        self._refresher_ckpt.stop()
        self._refresher_metric.stop()
        self.flush_check_point()
        self.flush_check_metric()
