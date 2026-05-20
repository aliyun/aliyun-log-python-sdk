# tests/samples/

Self-contained scripts that show how to call SLS APIs. They are **not**
collected by `pytest` (see `pyproject.toml`'s `testpaths`) and they are not
run in CI.

Most read credentials from the legacy `ALIYUN_LOG_SAMPLE_*` env vars and call
into a real endpoint. Treat them as runnable examples, not as tests.

```
python tests/samples/sample.py
```

If you are looking for a starter snippet, prefer `tests/examples/` for the
folder of curated, topic-grouped examples.
