# Testing

The Python SDK separates fast, hermetic unit tests from end-to-end tests that
require a real SLS endpoint.

## Layout

```
tests/
  _helpers/         shared env + mock infrastructure
  unit/             no network, runs in CI on every push
  e2e/              hits a real SLS endpoint, gated by env vars
  samples/          runnable example scripts (NOT collected by pytest)
  examples/         topic-grouped example scripts (NOT collected by pytest)
```

`pyproject.toml` sets `testpaths = ["tests/unit", "tests/e2e"]`, so anything
outside those two roots is ignored by `pytest` collection.

## Running unit tests

```sh
pip install -e .[test]
pytest tests/unit/ -v
```

Unit tests must not touch the network. The repo includes
`tests/unit/test_logclient_mock.py` as a worked example using `responses`.

## Running e2e tests

```sh
export LOG_TEST_ENDPOINT=cn-hangzhou.log.aliyuncs.com
export LOG_TEST_ACCESS_KEY_ID=...
export LOG_TEST_ACCESS_KEY_SECRET=...
export LOG_TEST_PROJECT=...
export LOG_TEST_LOGSTORE=...

pytest tests/e2e/ -v -m e2e
```

If any of the required vars are missing, every e2e test is auto-skipped.

The conftest also reads legacy variable names so older shell setups keep
working:

| Canonical (preferred)         | Legacy aliases (also accepted)                                                                  |
|-------------------------------|--------------------------------------------------------------------------------------------------|
| `LOG_TEST_ENDPOINT`           | `ALIYUN_LOG_SAMPLE_ENDPOINT`, `TEST_ENDPOINT`, `SLS_ENDPOINT`                                    |
| `LOG_TEST_ACCESS_KEY_ID`      | `ALIYUN_LOG_SAMPLE_ACCESSID`, `TEST_ACCESS_KEY_ID`, `SLS_AK_ID`                                  |
| `LOG_TEST_ACCESS_KEY_SECRET`  | `ALIYUN_LOG_SAMPLE_ACCESSKEY`, `TEST_ACCESS_KEY_SECRET`, `SLS_AK_KEY`                            |
| `LOG_TEST_PROJECT`            | `ALIYUN_LOG_SAMPLE_PROJECT`, `TEST_PROJECT`, `SLS_PROJECT`                                       |
| `LOG_TEST_LOGSTORE`           | `ALIYUN_LOG_SAMPLE_LOGSTORE`, `TEST_LOGSTORE`, `SLS_LOGSTORE`                                    |

## Writing new tests

### Unit tests with mocked SLS responses

Use `responses` plus the helpers in `tests/_helpers/fakes.py`:

```python
import re

import pytest
import responses

from aliyun.log import GetLogsRequest, LogException
from tests._helpers.fakes import error_response, make_client, mock_sls_response


@responses.activate
def test_my_query():
    client = make_client(endpoint="cn-mock.example.com", project="mock-proj")
    mock_sls_response(
        responses,
        "POST",
        re.compile(r"https?://mock-proj\.cn-mock\.example\.com.*?/logstores/store-1/logs.*"),
        status=200,
        body={"meta": {"count": 0, "progress": "Complete"}, "data": []},
    )
    req = GetLogsRequest("mock-proj", "store-1", 1700000000, 1700000100, query="*")
    assert client.get_logs(req).get_count() == 0


@responses.activate
def test_error_path():
    client = make_client()
    mock_sls_response(
        responses, "POST",
        re.compile(r"https?://.*?/logstores/.*"),
        status=400,
        body=error_response("ParameterInvalid", "bad"),
    )
    with pytest.raises(LogException):
        client.get_logs(GetLogsRequest("p", "s", 0, 1, "*"))
```

### E2E tests against a real endpoint

```python
import pytest

pytestmark = pytest.mark.e2e


def test_round_trip(e2e_client, sls_env):
    # `e2e_client` is an aliyun.log.LogClient built from LOG_TEST_* env vars.
    # `sls_env` exposes endpoint/project/logstore/etc.
    e2e_client.list_project(size=1)
```

The autouse env check skips the whole test cleanly when env vars are missing,
so contributors without SLS credentials can still run `pytest tests/unit/`.
