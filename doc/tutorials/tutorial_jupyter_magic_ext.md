# 使用日志服务Jupyter Notebook扩展

## 支持环境

支持Python2/3下的:

- IPython Shell
- Jupyter Notebook (IPython Notebook)
- Jupyter Lab


## 安装
### 快速安装

**Jupyter Notebook**:

```shell
1. pip install aliyun-log-python-sdk>=0.6.43 pandas odps ipywidgets -U
```


**IPython Shell/Jupyter Lab**:

```shell
1. pip install aliyun-log-python-sdk>=0.6.43 pandas -U
```



### virtualenv下安装（举例）
```shell
2. virtualenv sls

# Jupyter Notebook:
3. pip install aliyun-log-python-sdk>=0.6.43 pandas odps ipywidgets -U

# IPython Shell/Jupyter Lab
3. pip install aliyun-log-python-sdk>=0.6.43 pandas -U

4. python -m ipykernel install --user --name=sls
```


### 启动
```
jupyter notebook 或者 jupyter lab 或者 ipython
```


## 配置

加载magic命令
```
%load_ext aliyun.log.ext.jupyter_magic
```

配置参数如下:
```
%manage_log <endpoint> <ak_id> <ak_key> <project> <logstore>
```

在Jupyter Notebook下，也可以无参数传入，通过GUI配置：
```
%manage_log
```

## 配置保存位置

将存储AK、Endpoint、Project、Logstore在~/.aliyunlogcli, 使用的块名是`__jupyter_magic__`

```ini
[__jupyter_magic__]
access-id=
access-key=
region-endpoint=
project=
logstore=
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

## Dataframe可视化增强
`Jupyter Notebook`下扩展了DataFrame的操作，表格进行了分页，也可以动态选择饼图、柱状图、线图、点图等可视化。

## 全量数据拉取
如果原始数据没有索引，无法使用查询统计时，或者不需要条件过滤时，可以使用拉取命令。

```shell
%fetch 2019-1-31 10:0:0+8:00 ~ 2019-1-31 10:00:10+8:00
```

**Note：**

1. 时间范围是服务器接受日志的时间，不同于日志自身的时间。
2. 拉取过程中，取消的话，已经拉取的数据会放到`log_df_part`中。

