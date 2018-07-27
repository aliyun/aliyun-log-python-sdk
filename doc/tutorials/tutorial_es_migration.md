# Elasticsearch 数据迁移

## 概述
使用 Python SDK 提供的 [MigrationManager](/aliyun/log/es_migration/migration_manager.py) 可以方便您快速将 Elasticsearch 中的数据导入日志服务。
MigrationManager 内部使用 [Scroll API](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html) 从 Elasticsearch 中抓取数据。

## 配置
| 参数 | 必选 | 说明 | 样例 |
| -------- | -------- | -------- | -------- |
| hosts | yes |elasticsearch 数据源地址列表，多个 host 之间用逗号分隔。 | "127.0.0.1:9200"<br>"localhost:9200,other_host:9200" |
| indexes | no | elasticsearch index 列表，多个 index 之间用逗号分隔，支持通配符。<br>默认抓取目标 es 中所有 index 的数据。 | "index1"<br>"my_index*,other_index" |
| query | no | 用于过滤文档，使用该参数您可以指定需要迁移的文档。<br>默认不会对文档进行过滤。 | '{"query": {"match": {"title": "python"}}}' |
| scroll | no | 用于告诉 elasticsearch 需要将查询上下文信息保留多长时间。<br>默认值为 5m。 | "5m" |
| endpoint | yes | 日志服务中用于存储迁移数据的 project 所属 endpoint。 | "cn-beijing.log.aliyuncs.com" |
| project_name | yes | 日志服务中用于存储迁移数据的 project。<br>需要您提前创建好。 | "your_project" |
| access_key_id | yes | 用户访问秘钥对中的 access_key_id。 | |
| access_key | yes | 用户访问秘钥对中的 access_key_secret。 | |
| logstore_index_mappings | no | 用于配置日志服务中的 logstore 和 elasticsearch 中的 index 间的映射关系。支持使用通配符指定 index，多个 index 之间用逗号分隔。<br>可选参数，默认情况下 logstore 和 index 是一一映射，这里允许用户将多个index 上的数据发往一个 logstore。 | '{"logstore1": "my_index\*", "logstore2": "index1,index2"}, "logstore3": "index3"}'<br>'{"your_logstore": "*"}'  |
| pool_size | no | 指定用于执行迁移任务的进程池大小。<br>MigrationManager 会针对每个 shard 创建一个数据迁移任务，任务会被提交到进程池中执行。<br>默认为 min(10, shard_count)。 | 10 |
| time_reference | no | 将 elasticsearch 文档中指定的字段映射成日志的 time 字段。<br>默认使用当前时间戳作为日志 time 字段的值。 | "field1" |
| source | no | 指定日志的 source 字段的值。<br>默认值为参数 hosts 的值。 | "your_source" |
| topic | no | 指定日志的 topic 字段的值。<br>默认值为空。 | "your_topic" |
| wait_time_in_secs | no | 指定 logstore、索引创建好后，MigrationManager 执行数据迁移任务前需要等待的时间。<br>默认值为 60，表示等待 60s。 | 60 |

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
- 为了提高吞吐量，MigrationManager 会为每个 shard 创建一个数据迁移任务，并提交到内部进程池中执行。
- 当全部任务执行完成后，migrate 方法才会退出。

## 任务执行情况展示
MigrationManager 使用 logging 记录任务的执行情况，您可以通过如下配置指定将结果输出至控制台。
```
logger = logging.getLogger()
logger.setLevel(logging.INFO)
ch = logging.StreamHandler(sys.stdout)
logger.addHandler(ch)
```

- 单个迁移任务执行结果展示。
```
========Tasks Info========
...
task_id=1, slice_id=1, slice_max=10, hosts=localhost:9200, indexes=None, query=None, project=test-project, time_cost_in_seconds=128.71100688, status=CollectionTaskStatus.SUCCESS, count=129330, message=None
...

编号为 1 的迁移任务执行成功，耗时 128.7s，迁移文档数量 129330。
```

- 迁移任务执行结果汇总信息。
```
========Summary========
Total started task count: 10
Successful task count: 10
Failed task count: 0
Total collected documentation count: 1000000

MigrationManager 总共启动了 10 个数据数据迁移任务，全部执行成功。迁移文档总数 1000000。
```

## 使用样例
- 将 hosts 为 `localhost:9200` 的 Elasticsearch 中的所有文档导入日志服务的项目 `project1` 中。
```
migration_manager = MigrationManager(hosts="localhost:9200",   
                                     endpoint=endpoint,
                                     project_name="project1",
                                     access_key_id=access_key_id,
                                     access_key=access_key)
migration_manager.migrate()
```

- 指定将 Elasticsearch 中索引名以 `myindex_` 开头的数据写入日志库 `logstore1`，将索引 `index1,index2` 中的数据写入日志库 `logstore2` 中。
```
migration_manager = MigrationManager(hosts="localhost:9200,other_host:9200",
                                     endpoint=endpoint,
                                     project_name="project1",
                                     access_key_id=access_key_id,
                                     access_key=access_key,
				     logstore_index_mappings='{"logstore1": "myindex_*", "logstore2": "index1,index2"}}')
migration_manager.migrate()
```

- 使用参数 query 指定从 Elasticsearch 中抓取 `title` 字段等于 `python` 的文档，并使用文档中的字段 `date1` 作为日志的 time 字段。
```
migration_manager = MigrationManager(hosts="localhost:9200",
                                     endpoint=endpoint,
                                     project_name="project1",
                                     access_key_id=access_key_id,
                                     access_key=access_key,
				     query='{"query": {"match": {"title": "python"}}}',
				     time_reference="date1")
migration_manager.migrate()
```

## 常见问题
**Q**：是否支持抓取特定时间范围内的 ES 数据？

**A**：ES 本身并没有内置 time 字段，如果文档中某个字段代表时间，可以使用参数 query 进行过滤。


