#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

from .logresponse import LogResponse
from .queriedlog import QueriedLog
from .logexception import LogException
from .util import value_or_default
from enum import Enum

class GetLogsV3Response(LogResponse):
    """ The response of the GetLog API from log.

    :type resp: dict
    :param resp: GetLogsV3Response HTTP response body

    :type header: dict
    :param header: GetLogsV3Response HTTP response header
    """

    def __init__(self, resp, header):
        LogResponse.__init__(self, header, resp)
        try:
            self.meta = GetLogsV3Response.GetLogsV3ResponseMeta(
                resp.get("meta"))
            self.logs = []
            for log in resp["data"]:
                self.logs.append(QueriedLog.from_dict(log))

        except Exception as ex:
            raise LogException("InvalidResponse",
                               "Failed to parse GetLogV3Response, \nheader: "
                               + str(header) + " \nBody:"
                               + str(resp) + " \nOther: " + str(ex),
                               resp_header=header,
                               resp_body=resp)

    def get_meta(self):
        """ Get meta from the response

        :return: meta, GetLogsV3ResponseMeta
        """
        return self.meta

    def get_logs(self):
        """ Get logs from the response
        :return: logs, [QueriedLog]
        """
        return self.logs

    def get_count(self):
        """ Get number of logs of response
        """
        return self.get_meta().get_count()

    def is_completed(self):
        """ Check if the get logs query is completed

        :return: bool, true if this logs query is completed
        """
        return self.get_meta().is_completed()

    def to_dict(self):
        return {
            'meta': self.get_meta().to_dict(),
            'data': [log.to_dict() for log in self.logs]
        }

    def log_print(self):
        print(self.to_dict())

    def merge(self, other):
        """ merge with other GetLogsV3Response
            merge logs and meta
        """
        if not isinstance(other, GetLogsV3Response):
            raise ValueError(
                "passed response is not a GetLogsV3Response: " + str(type(other)))
        if other is None:
            return self
        self.meta.merge(other.get_meta())
        self.logs.extend(other.get_logs())
        return self

    class GetLogsV3ResponseMeta():
        """ The meta info of get logs response

        """

        def __init__(self, meta):
            self.count = value_or_default(meta.get("count"), 0)
            self.progress = meta.get("progress")
            self.processed_rows = value_or_default(
                meta.get("processedRows"), 0)
            self.elapsed_millisecond = meta.get("elapsedMillisecond")
            self.has_sql = meta.get("hasSQL")
            self.where_query = meta.get("whereQuery")
            self.agg_query = meta.get("aggQuery")
            self.cpu_sec = meta.get("cpuSec")
            self.cpu_cores = meta.get("cpuCores")
            self.mode = value_or_default(meta.get("mode"), 0)
            self.scan_bytes = meta.get("scanBytes")
            self.limited = meta.get("limited")
            self.processed_bytes = meta.get("processedBytes")
            self.telemetry_type = meta.get("telementryType")  # not typo
            self.power_sql = value_or_default(meta.get("powerSql"), False)
            self.inserted_sql = meta.get("insertedSQL")
            self.keys = meta.get("keys")
            self.marker = meta.get("marker")
            self.shard = meta.get("shard")
            self.is_accurate = meta.get("isAccurate")
            self.column_types = meta.get("columnTypes")
            self.highlights = meta.get("highlights")

            phrase_query_info = meta.get("phraseQueryInfo")
            if phrase_query_info is not None:
                self.phrase_query_info = GetLogsV3Response.PhraseQueryInfo(
                    phrase_query_info)
            else:
                self.phrase_query_info = None

            self.terms = []
            for term in meta.get("terms", []):
                self.terms.append(GetLogsV3Response.Term.from_dict(term))

        def to_dict(self):
            """ to Dict
            """
            phrase_query_info = None
            if self.phrase_query_info is not None:
                phrase_query_info = self.phrase_query_info.to_dict()

            terms = [term.to_dict() for term in self.terms]

            return {
                'count': self.count,
                'progress': self.progress,
                'processedRows': self.processed_rows,
                'elapsedMillisecond': self.elapsed_millisecond,
                'hasSQL': self.has_sql,
                'whereQuery': self.where_query,
                'aggQuery': self.agg_query,
                'cpuSec': self.cpu_sec,
                'cpuCores': self.cpu_cores,
                'mode': self.mode,
                'scanBytes': self.scan_bytes,
                'limited': self.limited,
                'processedBytes': self.processed_bytes,
                'telementryType': self.telemetry_type,  # not typo
                'powerSql': self.power_sql,
                'insertedSQL': self.inserted_sql,
                'keys': self.keys,
                'marker': self.marker,
                'shard': self.shard,
                'isAccurate': self.is_accurate,
                'columnTypes': self.column_types,
                'highlights': self.highlights,
                'phraseQueryInfo': phrase_query_info,
                'terms': terms,
            }

        def log_print(self):
            """ print infos
            """
            print(self.to_dict())

        def merge(self, other):
            """ merge with other GetLogsV3ResponseMeta

            """
            self.progress = other.progress
            self.count += other.count
            self.processed_rows += other.processed_rows
            return self

        def get_count(self):
            """ Get log number from the response

            :return: int, log number
            """
            return self.count

        def get_progress(self):
            """ Progress of get logs, returns 'Complete' if completed

            :return: str
            """
            return self.progress

        def is_completed(self):
            """ Check if the get logs query is completed

            :return: bool, true if this logs query is completed
            """
            return self.progress == 'Complete'

        def get_processed_rows(self):
            """ Get processed rows from the response

            :return: processed_rows, int
            """
            return self.processed_rows

        def get_elapsed_millisecond(self):
            """ Get elapsed mills from the response

            :return: elapsed_millisecond, int
            """
            return self.elapsed_millisecond

        def get_has_sql(self):
            """ Get whether has sql from the response

            :return: has_sql, boolean
            """
            return self.has_sql

        def get_where_query(self):
            """ Get the Search part of "Search|Analysis"

            :return: where_query, str
            """
            return self.where_query

        def get_agg_query(self):
            """ Get the Analysis part of "Search|Analysis"

            :return: agg_query, str
            """
            return self.agg_query

        def get_cpu_sec(self):
            """ Get cpu seconds used from the response

            :return: cpu_sec, float64
            """
            return self.cpu_sec

        def get_cpu_cores(self):
            """ Get cpu cores used from the response

            :return: cpu_cores, float64
            """
            return self.cpu_cores

        def get_mode(self):
            """ Get query mode from the response

            :return: mode
            """
            return self.mode

        def get_query_mode(self):
            """ Get query mode from the response

            :return: QueryMode
            """
            return GetLogsV3Response.QueryMode(self.mode)

        def get_scan_bytes(self):
            """ Get scan_bytes from the response

            :return: scan_bytes, int
            """
            return self.scan_bytes

        def get_limited(self):
            """ 
            :return: limited, int
            """
            return self.limited

        def get_processed_bytes(self):
            """ 
            :return: processed_bytes, int
            """
            return self.processed_bytes

        def get_telemetry_type(self):
            """ 
            :return: telemetry_type, str
            """
            return self.telemetry_type

        def get_power_sql(self):
            """ 
            :return: power_sql, bool
            """
            return self.power_sql

        def get_inserted_sql(self):
            """ 
            :return: inserted_sql, str
            """
            return self.inserted_sql

        def get_keys(self):
            """ 
            :return: keys, List[str]
            """
            return self.keys

        def get_terms(self):
            """ 
            :return: terms, List[Term]
            """
            return self.terms

        def get_marker(self):
            """ 
            :return: marker, str
            """
            return self.marker

        def get_shard(self):
            """ 
            :return: shard, int
            """
            return self.shard

        def get_is_accurate(self):
            """ 
            :return: is_accurate, bool
            """
            return self.is_accurate

        def get_column_types(self):
            """ 
            :return: column_types, List[str]
            """
            return self.column_types

        def get_highlights(self):
            """ 
            :return: highlights, List[Dict]
            """
            return self.highlights

        def get_phrase_query_info(self):
            """ 
            :return: phraseQueryInfo, PhraseQueryInfo
            """
            return self.phrase_query_info

    class QueryMode(Enum):
        """ The enum of query type"""
        NORMAL = 0
        PHRASE = 1
        SCAN = 2
        SCAN_SQL = 3

    class Term():
        """ terms of query, field key/term
        """

        def __init__(self, key, term):
            self.key = key
            self.term = term

        @classmethod
        def from_dict(cls, data):
            """ Initialize from a dict
            """
            key = data.get("key")
            term = data.get("term")
            return cls(key, term)

        def get_key(self):
            """ field key of term
            """
            return self.key

        def get_term(self):
            """ field term of term
            """
            return self.term

        def to_dict(self):
            """ to Dict
            """
            return {
                "key": self.key,
                "term": self.term
            }

        def log_print(self):
            """ print info
            """
            print(self.to_dict())

    class PhraseQueryInfo():
        """ query info of phrase, includes beginOffset/endOffset/scanAll/endTime

        """

        def __init__(self, pharse_query_info):
            self.begin_offset = pharse_query_info.get("beginOffset")
            self.end_offset = pharse_query_info.get("endOffset")
            self.scan_all = pharse_query_info.get("scanAll")
            self.end_time = pharse_query_info.get("endTime")

        def get_begin_offset(self):
            """ Get begin_offset from the response

            :return: begin_offset, int
            """
            return self.begin_offset

        def get_end_offset(self):
            """ Get end_offset from the response

            :return: end_offset, int
            """
            return self.end_offset

        def get_scan_all(self):
            """ Get scan_all from the response

            :return: scan_all, bool
            """
            return self.scan_all

        def get_end_time(self):
            """ Get end_time  from the response

            :return: end_time, int
            """
            return self.end_time

        def to_dict(self):
            """ to Dict
            """
            return {
                "beginOffset": self.begin_offset,
                "endOffset": self.end_offset,
                "scanAll": self.scan_all,
                "endTime": self.end_time
            }

        def log_print(self):
            """ print info

            """
            print(self.to_dict())
