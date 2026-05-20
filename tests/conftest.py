# encoding: utf-8
"""Shared pytest fixtures and collection hooks for the test suite."""

from __future__ import absolute_import

import pytest

from tests._helpers.env import require_sls_env
from tests._helpers.fakes import make_client


def pytest_collection_modifyitems(config, items):
    """Attach an autouse-style env check to every test marked ``e2e``.

    We rewrite each e2e item's setup so it calls ``require_sls_env()`` before
    the test body runs; missing env vars cause the whole test to skip cleanly.
    """
    from tests._helpers.env import _read_env

    _, missing = _read_env()
    if not missing:
        return

    skip_e2e = pytest.mark.skip(
        reason="SKIP: requires SLS env, missing {0} (set LOG_TEST_*)".format(missing)
    )
    for item in items:
        if "e2e" in item.keywords:
            item.add_marker(skip_e2e)


@pytest.fixture
def sls_env():
    """Return the SLS env dict for an e2e test, or skip if missing."""
    return require_sls_env()


@pytest.fixture
def e2e_client(sls_env):
    """Return a real LogClient built from env vars, or skip if env missing."""
    from aliyun.log import LogClient

    return LogClient(
        sls_env["endpoint"],
        sls_env["access_key_id"],
        sls_env["access_key_secret"],
    )


@pytest.fixture
def mocked_client():
    """Return a LogClient pre-wired for ``responses``-based unit tests.

    Pair with ``responses.activate`` or a ``responses.RequestsMock`` context
    manager to stub the HTTP boundary.
    """
    return make_client()
