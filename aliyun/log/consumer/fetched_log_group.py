# -*- coding: utf-8 -×-


class FetchedLogGroup(object):

    def __init__(self, shard_id, data, end_cursor, log_group_count):
        self._shard_id = shard_id
        self._data = data
        self._end_cursor = end_cursor
        self._log_group_count = log_group_count

    @property
    def shard_id(self):
        return self._shard_id

    @property
    def data(self):
        return self._data

    @property
    def end_cursor(self):
        return self._end_cursor

    @property
    def log_group_size(self):
        return self._log_group_count
