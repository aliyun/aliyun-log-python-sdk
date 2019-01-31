# 使用日志服务Jupyter Notebook扩展

## 安装
### 快速安装
```shell
1. pip install git+https://github.com/aliyun/aliyun-log-python-sdk.git@master -U
2. pip install pandas[all] odps ipywidgets
3. jupyter notebook
```

### virtualenv下安装
```shell
1. pip install git+https://github.com/aliyun/aliyun-log-python-sdk.git@master -U
2. virtualenv sls_ext
3. pip install pandas[all] odps ipywidgets
4. python -m ipykernel install --user --name=sls
5. jupyter notebook
```

## 配置

加载magic命令
```
%load_ext aliyun.log.ext.jupyter_magic
```

配置参数，无参数（通过GUI）或者命令行传入需要的参数
```
%manage_log
# 或者
%manage_log <endpoint> <ak_id> <ak_key> <project> <logstore>
```

## 查询

### 快速查询与统计（过去15分钟）
```
%log SLS查询分析语句
```

### 一般查询域统计（配置时间）

第一行用`from_time ~ to_time`这样的格式操作。
**注意：** 两个`%`

```sql
%%log -1day ~ now
* |
select date_format(date_trunc('hour', __time__), '%H:%i') as dt,
        count(1)%100 as pv,
        round(sum(if(status < 400, 1, 0))*100.0/count(1), 1) AS ratio
        group by date_trunc('hour', __time__)
        order by dt limit 1000

```

**Note：**如果只有查询的部分，会自动拉取时间范围内所有日志（自动分页）

## Dataframe操作
查询返回值通过`log_df`进行操作。是一个`Pandas`的标准`DataFrame`

操作示例：
```sell
%matplotlib inline
import matplotlib.pyplot as plt
plt.rcParams["figure.figsize"] = (12, 9)
import seaborn as sns

%log host: www.a?.mock* | select body_bytes_sent, host limit 100000

log_df['body_bytes_sent'] = log_df['body_bytes_sent'].astype(int)
sns.boxenplot(x='host', y='body_bytes_sent', data=log_df);
```

## 全量数据拉取
如果原始数据没有索引，无法使用查询统计时，或者不需要条件过滤时，可以使用拉取命令。

```shell
%fetch 2019-1-31 10:0:0+8:00 ~ 2019-1-31 10:00:10+8:00
```

**Note：**

1. 时间范围是服务器接受日志的时间，不同于日志自身的时间。
2. 拉取过程中，取消的话，已经拉取的数据会放到`log_df_part`中。

