# Release History

## 0.9.35 (2025-11-05)

**Feature**

- support logtail pipeline config api

## 0.9.34 (2025-10-29)

**Feature**

- consumer support preprocess and json processor

## 0.9.31 (2025-08-15)

**Feature**

- support async sql

## 0.9.29 (2025-09-03)

**Fixing**

- fix tag api hostname

## 0.9.28 (2025-08-07)

**Feature**

- support soft delete
- support spl consume processor

## 0.9.27 (2025-07-28)

**Feature**

- support copy alert and dashboard

## 0.9.26 (2025-07-18)

**Feature**

- support pg external store

## 0.9.25(2025-07-09)

**Feature**

- support storeview api

## 0.9.24 (2025-06-17)

**Feature**

- support metering mode api

## 0.9.23 (2025-06-12)

**Feature**

- support zstd compression

## 0.9.20 (2025-05-14)

**Feature**

- default use http long connection

## 0.9.16 (2025-04-19)

**Feature**

- use lz4 instead of lz4a

## 0.9.4 (2024-07-01)

**Fixing**

- fix lib lz4 detect

## 0.9.3 (2024-06-12)

**Fixing**

- fix get_log query_info_str parse error when empty

## 0.9.2 (2024-05-10)

**Feature**

- get_log use getLogStoreLogsV2 api

**Breaking Changes**

- get_log use getLogStoreLogsV2 api, there are a few breaking changes in the class GetLogsResponse.
  get_body() now returns `{"meta": {}, "data": []}` instead of `[]`, to get logs please use method `.get_logs()`.

## 0.8.10 (2023-09-21)

This version introduces some unexpected breaking changes which will be fixed in the future.

**Feature**

- support nano time when write logs

**Unexpcted Breaking Changes**

- The constructor of class LogItem.

## 0.7.2 (2021-10-25)

----------------------------
**Fixing**

- fix job type in list alert
- fix ext_info in resource apis

## 0.7.0 (2021-10-17)

----------------------------
**Implementations**

- Add resources api
- Update elasticsearch migration to use scroll
- fix version of dependencies  to keep compatible with Python 2: protobuf<=3.17.3, dateparser<=0.7.6

## 0.6.57 (2021-09-09)

----------------------------
**Implementations**

- Add executeSQl apis
- Add extra parameter power_sql in get_log/get_logs/get_log_all
- Add SQL related headers(x-log-**) on GetLogResponse

**Fixing**

- fix typo

## 0.6.48 (2020-03-20)

----------------------------
**Implementations**

- Support Elasticsearch v7.x in es_migration
- Resume break-point when restart in es_migration
- Report state logging into SLS for es_migration

**Break Change**

- Elasticsearch v6.x is no longer supported in es_migration

## 0.6.6 (2017-11-10)

----------------------------
**Implementations**

- Add support for Consumer Group

**Newly Supported Python**

- PyPy2
- PyPy3

**Fixing**

- Support case insensitive in header name via REST

## 0.6.5 (2017-10-30)

----------------------------
**Implementations**

- Add support for Python 3.x

**Supported Python**

- Python 2.6
- Python 2.7
- Python 3.3
- Python 3.4
- Python 3.5
- Python 3.6

## 0.6.4 (2017-10-25)

----------------------------
**Improvements**

- Uploaded to Pypi to support pip installation

## 0.6.0 (2015-11-16)

----------------------------
**Implementations**

- Wrap Rest API
- Implement the signature of API request
- Use format of Protocol Buffer to transfer data
- Support API defined compression style
- Support API to do batch query and data consumption
- Use exception to uncover errors

**Supported Python**

- Python 2.6
- Python 2.7

**Supported API**

- Log Service API 0.6
