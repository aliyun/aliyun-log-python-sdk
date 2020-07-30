# Release History

### 0.6.48 (2020-03-20)
----------------------------
**Implementations**
- Support Elasticsearch v7.x in es_migration
- Resume break-point when restart in es_migration
- Report state logging into SLS for es_migration

**Break Change**
- Elasticsearch v6.x is no longer supported in es_migration


### 0.6.6 (2017-11-10)
----------------------------
**Implementations**
- Add support for Consumer Group

**Newly Supported Python**
- PyPy2
- PyPy3

**Fixing**
- Support case insensitive in header name via REST


### 0.6.5 (2017-10-30)
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

### 0.6.4 (2017-10-25)
----------------------------
**Improvements**
- Uploaded to Pypi to support pip installation


### 0.6.0 (2015-11-16)
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
