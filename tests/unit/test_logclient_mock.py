# encoding: utf-8
"""Demo unit tests proving the responses-based mock infrastructure works.

These tests must NOT touch the network. They run as part of `pytest tests/unit`
in CI, with no SLS env vars configured.
"""

from __future__ import absolute_import

import json
import re

import pytest
import responses

from aliyun.log import GetLogsRequest, LogClient, LogException, LogItem, PutLogsRequest

from tests._helpers.fakes import error_response, make_client, mock_sls_response


@responses.activate
def test_get_logs_returns_parsed():
    """GET /logs response is parsed into a GetLogsResponse with logs."""
    client = make_client(endpoint="cn-mock.example.com", project="mock-proj")

    body = {
        "meta": {
            "count": 1,
            "progress": "Complete",
            "processedRows": 1,
            "elapsedMillisecond": 1,
            "hasSQL": False,
            "whereQuery": "",
            "aggQuery": "",
            "cpuSec": 0,
            "cpuCores": 0,
            "keys": ["k"],
            "terms": [],
            "marker": "",
            "mode": 0,
            "phraseQueryInfo": {"scanAll": False, "beginOffset": 0, "endOffset": 0},
            "shard": -1,
            "scanBytes": 0,
            "isAccurate": False,
            "limited": 0,
            "telementryType": "",
        },
        "data": [
            {"__time__": "1700000000", "__source__": "src1", "k": "v"},
        ],
    }

    mock_sls_response(
        responses,
        "POST",
        re.compile(r"https?://mock-proj\.cn-mock\.example\.com.*?/logstores/store-1/logs.*"),
        status=200,
        body=body,
    )

    req = GetLogsRequest(
        project="mock-proj",
        logstore="store-1",
        fromTime=1700000000,
        toTime=1700000100,
        query="*",
    )
    resp = client.get_logs(req)
    assert resp.get_count() == 1
    log = resp.get_logs()[0]
    assert log.contents.get("k") == "v"
    assert log.source == "src1"


@responses.activate
def test_put_logs_signs_request():
    """PutLogs sends a protobuf body and the SLS auth header is set."""
    client = make_client(endpoint="cn-mock.example.com", project="mock-proj")

    captured = {}

    def request_callback(request):
        captured["headers"] = dict(request.headers)
        captured["body"] = request.body
        return (200, {"x-log-requestid": "mock-request-id"}, "")

    responses.add_callback(
        responses.POST,
        re.compile(r"https?://mock-proj\.cn-mock\.example\.com.*?/logstores/store-1/shards/lb"),
        callback=request_callback,
    )

    item = LogItem(timestamp=1700000000, contents=[("k", "v")])
    client.put_logs(PutLogsRequest("mock-proj", "store-1", "topic", "src", [item]))

    headers = captured["headers"]
    # Authorization header is what SLS uses for AuthV1; AuthV4 sets x-acs-content-sha256.
    has_auth = "Authorization" in headers or "authorization" in headers
    assert has_auth, "expected an Authorization header on the signed request"
    # Body is protobuf -- header should declare it.
    assert headers.get("Content-Type") == "application/x-protobuf"
    # Body must be non-empty bytes.
    assert captured["body"]


@responses.activate
def test_error_response_raises_logexception():
    """A 400 with the SLS error envelope is converted to LogException with the right errorCode."""
    client = make_client(endpoint="cn-mock.example.com", project="mock-proj")

    mock_sls_response(
        responses,
        "POST",
        re.compile(r"https?://mock-proj\.cn-mock\.example\.com.*?/logstores/store-1/logs.*"),
        status=400,
        body=error_response("ParameterInvalid", "bad query"),
    )

    req = GetLogsRequest(
        project="mock-proj",
        logstore="store-1",
        fromTime=1700000000,
        toTime=1700000100,
        query="*",
    )
    with pytest.raises(LogException) as excinfo:
        client.get_logs(req)
    assert excinfo.value.get_error_code() == "ParameterInvalid"
