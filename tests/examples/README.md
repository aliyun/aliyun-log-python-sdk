# tests/examples/

Topic-grouped example scripts demonstrating common SLS workflows. These are
**not** collected by `pytest` and are not run in CI.

Folders:

- `consumer_group_examples/` — consumer group patterns (keyword monitor,
  copy data, sync to splunk/syslog).
- `export_examples/` — sinks for ODPS / OSS.
- `rebuild_index_examples/` — re-index helpers.
- `jupyter_magic_test/` — Jupyter notebooks for the `%log` magic.
- `test_migration_manager*.py` — long-running migration scripts.

Each script reads its own credentials/config; treat them as walkthroughs.
