# 使用日志服务Jupyter Notebook扩展

# 背景

## IPython/Jupyter很流行
[Jupyter](https://jupyter.org/)的前身是IPython Notebook，而IPython Notebook的前身是IPython。如下可以看到起[发展轨迹](https://en.wikipedia.org/wiki/Project_Jupyter)：

![image](https://yqfile.alicdn.com/11fbbd9dc6c2a4eb00ff0a5816eddfef79dc707b.png)

**IPython/Jupyter非常流行，从三个方面可以看到：**
* 数据科学领域Python愈来愈流行已经是既定事实，根据[数据科学与机器学习社区Kaggle 2018年调查](https://www.kaggle.com/erikbruin/r-vs-python-and-kmodes-clustering-2018-survey)，超过92%的人员会使用Python，而IPython/Jupyter也已经是不争的Python科学生态入口，使用Python做数据分析的人都会选择IPython/Jupyter作为工具平台。
* IPython/Jupyter Notebook不只是Python独有，作为开放平台，已经支持超过[50种语言](https://github.com/jupyter/jupyter/wiki/Jupyter-kernels)，例如Go、Java等。
* 各大云厂商都提供了对于Notebook的支持，SaaS生态中也有许多Notebook的有用工具，例如[Github](https://help.github.com/articles/working-with-jupyter-notebook-files-on-github/)、[NBViewer](https://nbviewer.jupyter.org/)等。

![image](https://yqfile.alicdn.com/6db3b31310ba33485a71818edb2f4e69a1dafc86.png)

## 日志服务对IPython/Jupyter支持
[阿里云的日志服务（log service）](https://www.aliyun.com/product/sls/)是针对日志类数据的一站式服务，无需开发就能快捷完成海量日志数据的采集、消费、投递以及查询分析等功能。通过日志服务对IPython/Jupyter扩展的支持，可以轻松地使用Python对海量数据进行深度加工（ETL）、交互式分析（通过SQL、DataFrame）、机器学习与可视化等：

![image](https://yqfile.alicdn.com/05e16380e08b9444c565e720f6d7c61e66737b93.png)

# 功能概述

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

更多安装问题可以参考[这里](https://github.com/aliyun/aliyun-log-python-sdk/blob/master/README_CN.md#%E5%AE%89%E8%A3%85)

## 配置

![img](https://img.alicdn.com/tfs/TB17rWWF4TpK1RjSZFMXXbG_VXa-494-376.gif)

**加载magic命令**
```
%load_ext aliyun.log.ext.jupyter_magic
```

**配置参数如下:**
```
%manage_log <服务入口> <秘钥ID> <秘钥值> <日志项目名> <日志库名>
```

**在Jupyter Notebook下，也可以无参数传入，通过GUI配置：**
```
%manage_log
```

关于服务入口、秘钥等，可以进一步参考[配置](https://help.aliyun.com/document_detail/29064.html)。


### 配置保存位置

以上操作将存储AK、Endpoint、Project、Logstore在~/.aliyunlogcli, 使用的块名是`__jupyter_magic__`

```ini
[__jupyter_magic__]
access-id=
access-key=
region-endpoint=
project=
logstore=
```

# 支持场景


## 1. 查询与统计

### 快速查询与统计（过去15分钟）

![img](https://img.alicdn.com/tfs/TB1hyaUF9zqK1RjSZFHXXb3CpXa-932-568.gif)

```
%log SLS查询分析语句
```

关于查询、扩展语法（SQL92标准），可以进一步参考[查询与统计](https://help.aliyun.com/document_detail/43772.html)

### 一般查询域统计（配置时间）

![img](https://img.alicdn.com/tfs/TB1o4WRF9zqK1RjSZPxXXc4tVXa-562-634.gif)


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

具体时间格式的支持，可以参考[这里](https://yq.aliyun.com/articles/621213)。

## 2. 全量数据拉取

![img](https://img.alicdn.com/tfs/TB1RXOVF3HqK1RjSZFgXXa7JXXa-466-514.gif)

如果原始数据没有索引，无法使用查询统计时，或者不需要条件过滤时，可以使用拉取命令。

```shell
%fetch 2019-1-31 10:0:0+8:00 ~ 2019-1-31 10:00:10+8:00
```

**Note：**

1. 时间范围是服务器接受日志的时间，不同于日志自身的时间。
2. 拉取过程中，取消的话，已经拉取的数据会放到`log_df_part`中。

## 3. Dataframe操作

![img](https://img.alicdn.com/tfs/TB11mO7F4jaK1RjSZFAXXbdLFXa-740-550.gif)


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

关于DataFrame操作，可以参考[Pandas DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/frame.html)。

## 4. Dataframe可视化增强
`Jupyter Notebook`下扩展了DataFrame的操作，表格进行了分页，也可以动态选择饼图、柱状图、线图、点图等可视化。

![img](https://img.alicdn.com/tfs/TB1hyaUF9zqK1RjSZFHXXb3CpXa-932-568.gif)


