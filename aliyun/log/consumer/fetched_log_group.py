# -*- coding: utf-8 -×-


class FetchedLogGroup(object):

    def __init__(self, shard_id, log_group_list, end_cursor):
        self._shard_id = shard_id
        self._fetched_log_group_list = log_group_list
        self._end_cursor = end_cursor

    @property
    def shard_id(self):
        return self._shard_id

    @property
    def fetched_log_group_list(self):
        return self._fetched_log_group_list

    @property
    def end_cursor(self):
        return self._end_cursor

    @property
    def log_group_size(self):
        return len(self._fetched_log_group_list.LogGroups)
