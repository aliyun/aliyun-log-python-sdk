#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

from .logresponse import LogResponse
from .queriedlog import QueriedLog
from .logexception import LogException
from .util import Util
from enum import Enum
import json

class GetLogsResponse(LogResponse):
    """ The response of the GetLog API from log.

    :type resp: dict
    :param resp: GetLogsResponse HTTP response body

    :type header: dict
    :param header: GetLogsResponse HTTP response header
    """

    class QueryMode(Enum):
        """ The enum of query type"""
        NORMAL = 0
        PHRASE = 1
        SCAN = 2
        SCAN_SQL = 3

    def __init__(self, resp, header):
        LogResponse.__init__(self, header, resp)
        try:
            self._meta = GetLogsResponse.GetLogsResponseMeta(
                resp.get("meta"))
            self._logs = []
            for log in resp["data"]:
                self._logs.append(QueriedLog._from_dict(log))

        except Exception as ex:
            raise LogException("InvalidResponse",
                               "Failed to parse GetLogResponse, \nheader: "
                               + str(header) + " \nBody:"
                               + str(resp) + " \nOther: " + str(ex),
                               resp_header=header,
                               resp_body=resp)

    @staticmethod
    def _from_v1_resp(resp, header):
        """
        Support old api with best effort
        """
        meta_dict = {
            'count': len(resp),
            'progress': Util.h_v_t(header, 'x-log-progress'),
            'processedRows': int(Util.h_v_td(header, 'x-log-processed-rows', '0')),
            'elapsedMillisecond': int(Util.h_v_td(header, 'x-log-elapsed-millisecond', '0')),
            'hasSQL': (Util.h_v_td(header, 'x-log-has-sql', 'false') != 'false'),
            'whereQuery': Util.h_v_td(header, 'x-log-where-query', ''),
            'aggQuery': Util.h_v_td(header, 'x-log-agg-query', ''),
            'cpuSec': float(Util.h_v_td(header, 'x-log-cpu-sec', '0')),
            'cpuCores': int(Util.h_v_td(header, 'x-log-cpu-cores', '0')),
        }
        # parse query info
        query_info_str = Util.h_v_td(header, 'x-log-query-info', '')
        if query_info_str != '':
            query_info = json.loads(query_info_str)
            meta_dict['mode'] = Util.h_v_td(query_info, 'mode', 0)
            query_mode = GetLogsResponse.QueryMode(meta_dict['mode'])
            if query_mode in (GetLogsResponse.QueryMode.SCAN, GetLogsResponse.QueryMode.SCAN_SQL):
                meta_dict['scanBytes'] = Util.h_v_td(query_info, 'scanBytes', 0)
            if query_mode in (GetLogsResponse.QueryMode.PHRASE, GetLogsResponse.QueryMode.SCAN):
                scan_query_info = Util.h_v_td(query_info, 'phraseQueryInfo', dict())
                meta_dict['phraseQueryInfo'] = {
                    'scanAll': (Util.h_v_td(scan_query_info, 'scanAll', 'false') != 'false'), 
                    'beginOffset': int(Util.h_v_td(scan_query_info, 'beginOffset', '0')),
                    'endOffset': int(Util.h_v_td(scan_query_info, 'endOffset', '0'))
                }

        return GetLogsResponse({
            "meta": meta_dict,
            "data": resp
        }, header)
    
    def get_count(self):
        """ Get log number from the response

        :return: int, log number
        """
        return self.get_meta().get_count()

    def is_completed(self):
        """ Check if the get logs query is completed

        :return: bool, true if this logs query is completed
        """
        return self.get_meta().is_completed()

    def get_logs(self):
        """ Get all logs from the response

        :return: QueriedLog list, all log data
        """
        return self._logs

    def get_processed_rows(self):
        """ Get processed rows from the response

        :return: int, processed_rows
        """
        return self.get_meta().get_processed_rows()

    def get_elapsed_mills(self):
        """ Get elapsed mills from the response

        :return: int, elapsed_mills 
        """
        return self.get_meta().get_elapsed_millisecond()

    def get_has_sql(self):
        """ Get whether has sql from the response

        :return: boolean, has_sql
        """
        return self.get_meta().get_has_sql()

    def get_where_query(self):
        """ Get the Search part of "Search|Analysis"

        :return: str, where_query
        """
        return self.get_meta().get_where_query()

    def get_agg_query(self):
        """ Get the Analysis part of "Search|Analysis"

        :return: str, agg_query
        """
        return self.get_meta().get_agg_query()

    def get_cpu_sec(self):
        """ Get cpu seconds used from the response

        :return: float, cpu_sec
        """
        return self.get_meta().get_cpu_sec()

    def get_cpu_cores(self):
        """ Get cpu cores used from the response

        :return: int, cpu_cores
        """
        return self.get_meta().get_cpu_cores()

    def get_query_mode(self):
        """ Get query_mode from the response

        :return: GetLogsResponse.QueryMode, query_mode
        """
        
        return GetLogsResponse.QueryMode(self.get_meta().get_mode())

    def get_scan_bytes(self):
        """ Get scan_bytes from the response

        :return: int, scan_bytes
        """
        return self.get_meta().get_scan_bytes()

    def get_begin_offset(self):
        """ Get begin_offset from the response

        :return: int, begin_offset
        """
        phrase_query_info = self.get_meta().get_phrase_query_info()
        return phrase_query_info.get_begin_offset() if phrase_query_info else None

    def get_end_offset(self):
        """ Get end_offset from the response

        :return: int, end_offset
        """
        phrase_query_info = self.get_meta().get_phrase_query_info()
        return phrase_query_info.get_end_offset() if phrase_query_info else None

    def get_scan_all(self):
        """ Get scan_all from the response

        :return: bool, scan_all
        """
        phrase_query_info = self.get_meta().get_phrase_query_info()
        return phrase_query_info.get_scan_all() if phrase_query_info else None

    def log_print(self):
        print('GetLogsResponse:')
        print('headers:', self.get_all_headers())
        print('count:', self.get_count())
        print('progress:', self.get_meta().get_progress())
        print('meta:', self.get_meta()._to_dict())
        print("\nQueriedLog class:\n")
        for log in self.get_logs():
            log.log_print()
            print("\n")

    def get_log_list(self):
        print('count:', self.get_count())
        print('progress:', self.get_meta().get_progress())
        return self.get_logs()

    def get_meta(self):
        """ Get meta from the response

        :return: meta, GetLogsResponseMeta
        """
        return self._meta

    def merge(self, other):
        """ merge with other response
            merge logs and meta
        """
        if not isinstance(other, GetLogsResponse):
            raise ValueError(
                "passed response is not a GetLogsResponse: " + str(type(other)))
        if other is None:
            return self
        self.get_meta().merge(other.get_meta())
        self.get_logs().extend(other.get_logs())
        return self

    class GetLogsResponseMeta():
        """ The meta info of get logs response

        """

        def __init__(self, meta):
            self._count = Util.v_or_d(meta.get("count"), 0)
            self._progress = meta.get("progress")
            self._processed_rows = Util.v_or_d(meta.get("processedRows"), 0)
            self._elapsed_millisecond = Util.v_or_d(meta.get("elapsedMillisecond"), 0)
            self._has_sql = Util.v_or_d(meta.get("hasSQL"), False)
            self._where_query = Util.v_or_d(meta.get("whereQuery"), '')
            self._agg_query = Util.v_or_d(meta.get("aggQuery"))
            self._cpu_sec = Util.v_or_d(meta.get("cpuSec"), 0.0)
            self._cpu_cores = Util.v_or_d(meta.get("cpuCores"), 0)
            self._mode = Util.v_or_d(meta.get("mode"), 0)
            self._scan_bytes = Util.v_or_d(meta.get("scanBytes"), 0)
            phrase_query_info = meta.get("phraseQueryInfo")
            self._phrase_query_info = GetLogsResponse.PhraseQueryInfo(
                phrase_query_info) if phrase_query_info else None

            # self._limited = meta.get("limited")
            # self._processed_bytes = meta.get("processedBytes")
            # self._telemetry_type = meta.get("telementryType")  # not typo
            # self._power_sql = Util.v_or_d(meta.get("powerSql"), False)
            # self._inserted_sql = meta.get("insertedSQL")
            # self._keys = meta.get("keys")
            # self._marker = meta.get("marker")
            # self._shard = meta.get("shard")
            # self._is_accurate = meta.get("isAccurate")
            # self._column_types = meta.get("columnTypes")
            # self._highlights = meta.get("highlights")
            # self._terms = []
            # for term in meta.get("terms", []):
            #     self._terms.append(GetLogsResponse.Term._from_dict(term))

        def is_completed(self):
            """ Check if the get logs query is completed

            :return: bool, true if this logs query is completed
            """
            return self.get_progress() == 'Complete'

        def merge(self, other):
            """ merge with other GetLogsResponseMeta

            """
            self._progress = other.get_progress()
            self._count += other.get_count()
            self._processed_rows += other.get_processed_rows()
            return self

        def get_count(self):
            """ Get log number from the response

            :return: int, log number
            """
            return self._count

        def _to_dict(self):
            """ to Dict
            """
            phrase_query_info = self.get_phrase_query_info()
            phrase_query_info_dict = phrase_query_info._to_dict() if phrase_query_info is not None else None

            return {
                'count': self.get_count(),
                'progress': self.get_progress(),
                'processedRows': self.get_processed_rows(),
                'elapsedMillisecond': self.get_elapsed_millisecond(),
                'hasSQL': self.get_has_sql(),
                'whereQuery': self.get_where_query(),
                'aggQuery': self.get_agg_query(),
                'cpuSec': self.get_cpu_sec(),
                'cpuCores': self.get_cpu_cores(),
                'mode': self.get_mode(),
                'scanBytes': self.get_scan_bytes(),
                'phraseQueryInfo': phrase_query_info_dict,
                # 'limited': self._limited,
                # 'processedBytes': self._processed_bytes,
                # 'telementryType': self._telemetry_type,  # not typo
                # 'powerSql': self._power_sql,
                # 'insertedSQL': self._inserted_sql,
                # 'keys': self._keys,
                # 'marker': self._marker,
                # 'shard': self._shard,
                # 'isAccurate': self._is_accurate,
                # 'columnTypes': self._column_types,
                # 'highlights': self._highlights,
                # 'terms': [term.to_dict() for term in self._terms],
            }

        def log_print(self):
            """ print infos
            """
            print(self._to_dict())

        def get_progress(self):
            """ Progress of get logs, returns 'Complete' if completed

            :return: str
            """
            return self._progress

        def get_processed_rows(self):
            """ Get processed rows from the response

            :return: processed_rows, int
            """
            return self._processed_rows

        def get_elapsed_millisecond(self):
            """ Get elapsed mills from the response

            :return: elapsed_millisecond, int
            """
            return self._elapsed_millisecond

        def get_has_sql(self):
            """ Get whether has sql from the response

            :return: has_sql, boolean
            """
            return self._has_sql

        def get_where_query(self):
            """ Get the Search part of "Search|Analysis"

            :return: where_query, str
            """
            return self._where_query

        def get_agg_query(self):
            """ Get the Analysis part of "Search|Analysis"

            :return: agg_query, str
            """
            return self._agg_query

        def get_cpu_sec(self):
            """ Get cpu seconds used from the response

            :return: cpu_sec, float64
            """
            return self._cpu_sec

        def get_cpu_cores(self):
            """ Get cpu cores used from the response

            :return: cpu_cores, float64
            """
            return self._cpu_cores

        def get_mode(self):
            """ Get query mode from the response

            :return: mode
            """
            return self._mode

        def get_scan_bytes(self):
            """ Get scan_bytes from the response

            :return: scan_bytes, int
            """
            return self._scan_bytes

        def get_phrase_query_info(self):
            """ 
            :return: phraseQueryInfo, PhraseQueryInfo
            """
            return self._phrase_query_info

        # def get_limited(self):
        #     """ 
        #     :return: limited, int
        #     """
        #     return self._limited

        # def get_processed_bytes(self):
        #     """ 
        #     :return: processed_bytes, int
        #     """
        #     return self._processed_bytes

        # def get_telemetry_type(self):
        #     """ 
        #     :return: telemetry_type, str
        #     """
        #     return self._telemetry_type

        # def get_power_sql(self):
        #     """ 
        #     :return: power_sql, bool
        #     """
        #     return self._power_sql

        # def get_inserted_sql(self):
        #     """ 
        #     :return: inserted_sql, str
        #     """
        #     return self._inserted_sql

        # def get_keys(self):
        #     """ 
        #     :return: keys, List[str]
        #     """
        #     return self._keys

        # def get_marker(self):
        #     """ 
        #     :return: marker, str
        #     """
        #     return self._marker

        # def get_shard(self):
        #     """ 
        #     :return: shard, int
        #     """
        #     return self._shard

        # def get_is_accurate(self):
        #     """ 
        #     :return: is_accurate, bool
        #     """
        #     return self._is_accurate

        # def get_column_types(self):
        #     """ 
        #     :return: column_types, List[str]
        #     """
        #     return self._column_types

        # def get_highlights(self):
        #     """ 
        #     :return: highlights, List[Dict]
        #     """
        #     return self._highlights
        
        # def get_terms(self):
        #     """ 
        #     :return: terms, List[Term]
        #     """
        #     return self._terms

    class PhraseQueryInfo():
        """ query info of phrase, includes beginOffset/endOffset/scanAll/endTime

        """

        def __init__(self, pharse_query_info):
            self._begin_offset = pharse_query_info.get("beginOffset")
            self._end_offset = pharse_query_info.get("endOffset")
            self._scan_all = pharse_query_info.get("scanAll")
            self._end_time = pharse_query_info.get("endTime")

        def get_begin_offset(self):
            """ Get begin_offset from the response

            :return: begin_offset, int
            """
            return self._begin_offset

        def get_end_offset(self):
            """ Get end_offset from the response

            :return: end_offset, int
            """
            return self._end_offset

        def get_scan_all(self):
            """ Get scan_all from the response

            :return: scan_all, bool
            """
            return self._scan_all

        def get_end_time(self):
            """ Get end_time  from the response

            :return: end_time, int
            """
            return self._end_time

        def _to_dict(self):
            """ to Dict
            """
            return {
                "beginOffset": self.get_begin_offset(),
                "endOffset": self.get_end_offset(),
                "scanAll": self.get_scan_all(),
                "endTime": self.get_end_time()
            }

        def log_print(self):
            """ print info

            """
            print(self._to_dict())


    # class Term():
    #     """ terms of query, field key/term
    #     """

    #     def __init__(self, key, term):
    #         self._key = key
    #         self._term = term

    #     @classmethod
    #     def _from_dict(cls, data):
    #         """ Initialize from a dict
    #         """
    #         key = data.get("key")
    #         term = data.get("term")
    #         return cls(key, term)

    #     def get_key(self):
    #         """ field key of term
    #         """
    #         return self._key

    #     def get_term(self):
    #         """ field term of term
    #         """
    #         return self._term

    #     def _to_dict(self):
    #         """ to Dict
    #         """
    #         return {
    #             "key": self._key,
    #             "term": self._term
    #         }

    #     def log_print(self):
    #         """ print info
    #         """
    #         print(self._to_dict())