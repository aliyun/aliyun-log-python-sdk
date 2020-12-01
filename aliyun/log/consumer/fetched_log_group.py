# -*- coding: utf-8 -×-


class FetchedLogGroup(object):

    def __init__(self, shard_id, log_group_list, end_cursor, raw_data_sizes):
        self._shard_id = shard_id
        self._fetched_log_group_list = log_group_list
        self._end_cursor = end_cursor
        self._raw_data_sizes = raw_data_sizes

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
    def log_group_count(self):
        if isinstance(self._fetched_log_group_list, list):
            log_group_count = 0
            for log_group in self.fetched_log_group_list:
                log_group_count += len(log_group.LogGroups)
            return log_group_count
        else:
            return len(self._fetched_log_group_list.LogGroups)

    @property
    def fetched_log_raw_data_sizes(self):
        return self._raw_data_sizes
