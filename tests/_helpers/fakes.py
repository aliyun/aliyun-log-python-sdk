# encoding: utf-8
"""Mock helpers for unit tests that exercise LogClient without real network.

These wrap the third-party ``responses`` library so test code can stay short
and so SLS-shaped responses (errorCode/errorMessage envelope, x-log-* headers)
can be produced consistently.
"""

from __future__ import absolute_import

import json
import re


def make_client(
    endpoint="cn-mock.example.com",
    access_key_id="mock-id",
    access_key="mock-key",
    project="mock-proj",
):
    """Return a LogClient ready to be used inside a ``responses.activate`` context.

    The endpoint never gets contacted; ``responses`` intercepts requests by URL.
    """
    from aliyun.log import LogClient

    client = LogClient(endpoint, access_key_id, access_key)
    # Stash project on the client for tests that want a default.
    client.test_project = project  # not used by LogClient itself
    return client


def error_response(error_code, error_message, request_id="mock-request-id"):
    """Return a JSON string matching the SLS error envelope shape.

    Mirrors :class:`aliyun.log.logexception.LogException` so error parsing
    code paths can be exercised end-to-end against a mocked response.
    """
    return json.dumps({
        "errorCode": error_code,
        "errorMessage": error_message,
        "requestId": request_id,
    })


def mock_sls_response(rsps, method, url_pattern, status=200, body=None, headers=None):
    """Register a response on a ``responses`` RequestsMock.

    :param rsps: a ``responses.RequestsMock`` instance (the value yielded by
        ``with responses.RequestsMock() as rsps`` or the module itself when
        using ``@responses.activate``).
    :param method: HTTP method, e.g. ``"GET"`` / ``"POST"``.
    :param url_pattern: regex/string URL pattern (passed through to ``responses``).
    :param status: HTTP status code to return.
    :param body: bytes/str body. Dicts are JSON-encoded.
    :param headers: optional response headers; ``x-log-requestid`` is auto-added.

    Always sets ``Content-Type: application/json`` if not provided.
    """
    if isinstance(url_pattern, str) and ("*" in url_pattern or "?" in url_pattern):
        url_pattern = re.compile(url_pattern)

    if isinstance(body, dict):
        body = json.dumps(body)
    if body is None:
        body = ""

    final_headers = {"x-log-requestid": "mock-request-id"}
    if headers:
        final_headers.update(headers)
    if "Content-Type" not in final_headers:
        final_headers["Content-Type"] = "application/json"

    rsps.add(
        method=method,
        url=url_pattern,
        body=body,
        status=status,
        headers=final_headers,
    )
