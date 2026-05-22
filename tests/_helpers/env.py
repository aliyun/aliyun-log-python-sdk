# encoding: utf-8
"""Env-var driven helpers for e2e tests.

Tests that need a real SLS endpoint should call ``require_sls_env()`` (or use
the ``sls_env`` / ``e2e_client`` fixtures) so that runs without credentials
skip cleanly instead of failing.

Standardized variable names (preferred):

    LOG_TEST_ENDPOINT
    LOG_TEST_ACCESS_KEY_ID
    LOG_TEST_ACCESS_KEY_SECRET
    LOG_TEST_PROJECT
    LOG_TEST_LOGSTORE

For backward compatibility we also accept (in priority order after LOG_TEST_*):

    ALIYUN_LOG_SAMPLE_ENDPOINT  / _ACCESSID         / _ACCESSKEY        / _PROJECT / _LOGSTORE
    TEST_ENDPOINT               / TEST_ACCESS_KEY_ID/ TEST_ACCESS_KEY_SECRET / TEST_PROJECT / TEST_LOGSTORE
    SLS_ENDPOINT                / SLS_AK_ID         / SLS_AK_KEY        / SLS_PROJECT / SLS_LOGSTORE
"""

from __future__ import absolute_import

import os

import pytest


# Each entry: canonical key -> ordered list of env-var names to try.
_ENV_ALIASES = {
    "endpoint": [
        "LOG_TEST_ENDPOINT",
        "ALIYUN_LOG_SAMPLE_ENDPOINT",
        "TEST_ENDPOINT",
        "SLS_ENDPOINT",
    ],
    "access_key_id": [
        "LOG_TEST_ACCESS_KEY_ID",
        "ALIYUN_LOG_SAMPLE_ACCESSID",
        "TEST_ACCESS_KEY_ID",
        "SLS_AK_ID",
    ],
    "access_key_secret": [
        "LOG_TEST_ACCESS_KEY_SECRET",
        "ALIYUN_LOG_SAMPLE_ACCESSKEY",
        "TEST_ACCESS_KEY_SECRET",
        "SLS_AK_KEY",
    ],
    "project": [
        "LOG_TEST_PROJECT",
        "ALIYUN_LOG_SAMPLE_PROJECT",
        "TEST_PROJECT",
        "SLS_PROJECT",
    ],
    "logstore": [
        "LOG_TEST_LOGSTORE",
        "ALIYUN_LOG_SAMPLE_LOGSTORE",
        "TEST_LOGSTORE",
        "SLS_LOGSTORE",
    ],
}


def _read_env():
    """Return (env_dict, missing_keys)."""
    result = {}
    missing = []
    for key, names in _ENV_ALIASES.items():
        value = None
        for name in names:
            v = os.environ.get(name)
            if v:
                value = v
                break
        result[key] = value
        if not value:
            missing.append(key)
    return result, missing


def require_sls_env():
    """Return SLS env dict or skip the test if any required field is missing.

    :return: dict with keys endpoint, access_key_id, access_key_secret, project, logstore
    """
    env, missing = _read_env()
    if missing:
        pytest.skip(
            "SKIP: requires SLS env, missing {0} (set LOG_TEST_* or one of the legacy aliases)".format(missing)
        )
    return env


def e2e_client():
    """Build a real LogClient from env vars (skips if env not set)."""
    # Imported lazily so that unit tests don't require LogClient deps to load
    # when they import from this module transitively.
    from aliyun.log import LogClient

    env = require_sls_env()
    return LogClient(env["endpoint"], env["access_key_id"], env["access_key_secret"])
