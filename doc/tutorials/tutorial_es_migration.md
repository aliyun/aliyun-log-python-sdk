# Elasticsearch 数据迁移

## 概述
使用 Aliyun-Log-Python-SDK 提供的 [MigrationManager](https://github.com/aliyun/aliyun-log-python-sdk/blob/master/aliyun/log/es_migration/migration_manager.py)可以方便您快速将 Elasticsearch 中的数据导入日志服务。
MigrationManager 内部使用 [Scroll API](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html) 从 Elasticsearch 中抓取数据。

如何一步一步完成从 Elasticsearch 迁移数据到 SLS，请参考 [阿里云官方最佳实践](https://www.aliyun.com/acts/best-practice/preview?id=471965)。

## 配置

| 参数 | 必选 | 说明 | 样例 |
| -------- | -------- | -------- | -------- |
| cache_path | yes | 用于缓存迁移进度的本地文件位置，实现断点续传。当存在迁移缓存的时候，以下参数中`time_reference`更改无效。对新的迁移任务，请确认指定路径为空文件夹，以防迁移任务受干扰。 | /path/to/cache |
| cache_duration | no | 缓存有效时间，基于 elasticsearch scroll 实现。当前一次迁移操作退出时间长度超过该时间段时，缓存失效，不能继续使用于断点续传。默认值是 1d。 | 1d<br>20h |
| hosts | yes | elasticsearch 数据源地址列表，多个 host 之间用逗号分隔。 | "127.0.0.1:9200"<br>"localhost:9200,other_host:9200"<br>"user:secret@localhost:9200" |
| indexes | no | elasticsearch index 列表，多个 index 之间用逗号分隔，支持通配符(*)。<br>默认抓取目标 es 中所有 index 的数据。 | "index1"<br>"my_index*,other_index" |
| query | no | 用于过滤文档，使用该参数您可以指定需要迁移的文档。<br>默认不会对文档进行过滤。 | '{"query": {"match": {"title": "python"}}}' |
| endpoint | yes | 日志服务中用于存储迁移数据的 project 所属 endpoint。 | "cn-beijing.log.aliyuncs.com" |
| project_name | yes | 日志服务中用于存储迁移数据的 project。<br>需要您提前创建好。 | "your_project" |
| access_key_id | yes | 用户访问秘钥对中的 access_key_id。 | |
| access_key | yes | 用户访问秘钥对中的 access_key_secret。 | |
| logstore_index_mappings | no | 用于配置日志服务中的 logstore 和 elasticsearch 中的 index 间的映射关系。支持使用通配符指定 index，多个 index 之间用逗号分隔。<br>可选参数，默认情况下 logstore 和 index 是一一映射，这里允许用户将多个index 上的数据发往一个 logstore。 | '{"logstore1": "my_index\*", "logstore2": "index1,index2", "logstore3": "index3"}'<br>'{"your_logstore": "\*"}'  |
| pool_size | no | 指定用于执行迁移任务的进程池大小。<br>MigrationManager 会针对每个 shard 创建一个数据迁移任务，任务会被提交到进程池中执行。<br>默认为 min(10, shard_count)。 | 10 |
| time_reference | no | 将 elasticsearch 文档中指定的字段映射成日志的 time 字段。<br>默认使用当前时间戳作为日志 time 字段的值。 | "field1" |
| source | no | 指定日志的 source 字段的值。<br>默认值为参数 hosts 的值。 | "your_source" |
| topic | no | 指定日志的 topic 字段的值。<br>默认值为空。 | "your_topic" |
| batch_size | no | 批量写入 SLS 的日志数目。SLS 要求同时写入的一批数据不超过 512KB，而且不超过1024条。 | 1000 |
| wait_time_in_secs | no | 指定 logstore、索引创建好后，MigrationManager 执行数据迁移任务前需要等待的时间。<br>默认值为 60，表示等待 60s。 | 60 |
| auto_creation | no | 指定是否让 MigrationManager 为您自动创建好 logstore 和 索引。<br>默认值为 True，表示自动创建。 | True<br>False |
| retries_failed | no | 对出错的迁移任务进行重试的次数。 <br>默认值为 10。 | 10 |

> aliyun-log-python-sdk.readthedocs.io 无法正常显示表格，请参阅[tutorial_es_migration.md](https://github.com/aliyun/aliyun-log-python-sdk/blob/master/doc/tutorials/tutorial_es_migration.md)

## 数据映射
### logstore - index
MigrationManager 默认会将 Elasticsearch index 中的数据迁移至同名的 logstore 中，当然您也可以通过参数 logstore_index_mappings 指定将多个 index 中的数据迁移至一个 logstore。

logstore 不必事先创建，如果 MigrationManager 发现目标 logstore 未创建，会为您在指定的 project 下创建好。

### 数据类型映射
MigrationManager 会根据 Elasticsearch 的[数据类型](https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-types.html) 在index 对应的 logstore 中创建好索引。

- Core datatypes

| Elasticsearch | 日志服务 |
| -------- | -------- |
| text | text |
| keyword | text，不分词 |
| long | long |
| integer | long |
| short | long |
| byte | long |
| double | double |
| float | double |
| half_float | double |
| scaled_float | double |
| date | text |
| boolean | text，不分词 |
| binary | n/a |
| integer_range | json |
| float_range | json |
| long_range | json |
| double_range | json |
| date_range | json |
| ip_range | text，不分词 |

- Complex datatypes

| Elasticsearch | 日志服务 |
| -------- | -------- |
| Array datatype | n/a |
| Object datatype | json |
| Nested datatype | n/a |

- Geo datatypes

| Elasticsearch | 日志服务 |
| -------- | -------- |
| Geo-point datatype | text |
| Geo-Shape datatype | text |

- Specialised datatypes

| Elasticsearch | 日志服务 |
| -------- | -------- |
| IP datatype | text，不分词 |
| Completion datatype | n/a |
| Token count datatype | n/a |
| mapper-murmur3 | n/a |
| Percolator type | n/a |
| join datatype | n/a |

## 抓取模式
- 为了提高吞吐量，MigrationManager 会为每个 index 的每个 shard 创建一个数据迁移任务，并提交到内部进程池中执行。
- 当全部任务执行完成后，migrate 方法才会退出。

## 任务执行情况展示
MigrationManager 使用 logging 记录任务的执行情况，并将日志上报到SLS，迁移状态监控在SLS中更加便捷，位置在上文中输入的参数 project 里面的 logstore: `internal-es-migration-log`。该 logstore 会在任务执行时自动创建。

迁移程序开始后，在控制台中可以看到以下内容：
```
#migration: c34d9636f8934cc18b9727263c476b66
setup aliyun log service...
#pool_size: 3
#tasks: 12
migrate: {"task_id": 0, "es_index": "my_index_01", "es_shard": 0, "logstore": "my-logstore-01"}
migrate: {"task_id": 1, "es_index": "my_index_02", "es_shard": 0, "logstore": "my-logstore-02"}
migrate: {"task_id": 2, "es_index": "my_index_03", "es_shard": 0, "logstore": "my-logstore-03"}
migrate: {"task_id": 3, "es_index": "my_index_04", "es_shard": 0, "logstore": "my-logstore-04"}
migrate: {"task_id": 4, "es_index": "my_index_01", "es_shard": 1, "logstore": "my-logstore-01"}
migrate: {"task_id": 5, "es_index": "my_index_02", "es_shard": 1, "logstore": "my-logstore-02"}
>> state: {"total": 12, "finished": 1, "dropped": 0, "failed": 0}
>> state: {"total": 12, "finished": 2, "dropped": 0, "failed": 0}
migrate: {"task_id": 6, "es_index": "my_index_03", "es_shard": 1, "logstore": "my-logstore-03"}
migrate: {"task_id": 7, "es_index": "my_index_04", "es_shard": 1, "logstore": "my-logstore-04"}
>> state: {"total": 12, "finished": 3, "dropped": 0, "failed": 0}
migrate: {"task_id": 8, "es_index": "my_index_01", "es_shard": 2, "logstore": "my-logstore-01"}
```
`migration`是本次迁移程序的 ID，用于在SLS中检索相应日志。`pool_size`是并行执行迁移的进程数，`tasks`是总的任务数目。migrate 是任务开始执行，state是迁移的进度。在SLS中也可以检索到相应的日志条目：
```
__source__:  127.0.0.1
__tag__:__migration__:  c34d9636f8934cc18b9727263c476b66
__topic__:
dropped:  0
failed:  0
finished:  5
logging:  {"message": "State", "funcName": "migrate", "levelname": "INFO", "module": "migration_manager", "process": "12353", "thread": "140034546173760"}
total:  12
```

单个迁移任务执行进度日志：
```
__source__:  127.0.0.1
__tag__:__migration__:  c34d9636f8934cc18b9727263c476b66
__topic__:
checkpoint:  {"_id": "anBkzXABMIXHzcoem-t0", "offset": {"@timestamp": "2020-02-28T22:40:11"}}
es_index:  my_index
es_shard:  2
logging:  {"message": "Migration progress", "process": "12311", "thread": "140047528814400", "module": "migration_task", "funcName": "_run", "levelname": "INFO"}
logstore:  my_logstore
progress:  10230
status:  processing
task:  {"es_index": "my_index", "es_shard": 2, "logstore": "my_logstore", "time_reference": "@timestamp"}
task_id:  5
update_time:  2020-03-12T23:55:10
```
以上是在 id 为 c34d9636f8934cc18b9727263c476b66 的迁移程序中，task_id 为5的任务的执行状态。


## 使用样例

- 将 hosts 为 `localhost:9200` 的 Elasticsearch 中的所有文档导入日志服务的项目 `project1` 中。

```Python
config = MigrationConfig(
    cache_path='/path/to/cache',
    hosts="localhost:9200",
    endpoint=endpoint,
    project_name="project1",
    access_key_id=access_key_id,
    access_key=access_key,
)
manager = MigrationManager(config)
manager.migrate()
```

- 指定将 Elasticsearch 中索引名以 `myindex_` 开头的数据写入日志库 `logstore1`，将索引 `index1,index2` 中的数据写入日志库 `logstore2` 中。

```Python
config = MigrationConfig(
    cache_path='/path/to/cache',
    hosts="localhost:9200",
    endpoint=endpoint,
    project_name="project1",
    access_key_id=access_key_id,
    access_key=access_key,
    logstore_index_mappings='{"logstore1": "myindex_*", "logstore2": "index1,index2"}}'
)
migration_manager = MigrationManager(config)
migration_manager.migrate()
```

- 使用参数 query 指定从 Elasticsearch 中抓取 `title` 字段等于 `python` 的文档，并使用文档中的字段 `date1` 作为日志的 time 字段。

```Python
config = MigrationConfig(
    cache_path='/path/to/cache',
    hosts="localhost:9200",
    endpoint=endpoint,
    project_name="project1",
    access_key_id=access_key_id,
    access_key=access_key,
    query='{"query": {"match": {"title": "python"}}}',
    time_reference="date1",
)
migration_manager = MigrationManager(config)
migration_manager.migrate()
```

- 使用 HTTP 基本认证`user:secret@localhost:9200`，从 Elasticserch 中迁移数据。

```Python
config = MigrationConfig(
    cache_path='/path/to/cache',
    hosts="user:secret@localhost:9200",
    endpoint=endpoint,
    project_name="project1",
    access_key_id=access_key_id,
    access_key=access_key,
)
migration_manager = MigrationManager(config)
migration_manager.migrate()
```


## 常见问题

**Q**：是否支持抓取特定时间范围内的 ES 数据？

**A**：ES 本身并没有内置 time 字段，如果文档中某个字段代表时间，可以使用参数 query 进行过滤。

**Q**：如何使用断点续传？

**A**：调用参数中 cache_path 指定 checkpoint 的存放位置，当迁移程序中断后，重新打开时指定相同的 cache_path 便可以继续迁移任务，可以更改迁移参数，比如 pool_size，batch_size。

**Q**：time_reference 有什么作用？

**A**：参数 time_reference 用于标记日志在 SLS 中的时间戳，另外还用于在迁移过程中的 chenkpoint，在中断重启时快速定位到续传点。所以应尽可能指定 time_reference 参数。

**Q**：数据迁移的速度有多快？

**A**：同 region 下的 ESC 到 SLS 的迁移，单个 SLS shard， 单个 ES shard，pool_size 为1，可实现接近5M/s的迁移速度。可通过调整 SLS shard 和 pool_size 大小来达到提速。

**Q**：ES 中存储大量冷数据，index都是closed状态，全部打开会对服务器产生很大压力，怎么做迁移？

**A**：基于断点续传功能，分批进行迁移。在确定好迁移任务后，开启一部分index，执行迁移指令开始迁移，完成后关闭这些index，开启另一批index，执行完全相同的迁移命令，会自动连续执行新的迁移任务。分批依次执行，直到全部完成。
