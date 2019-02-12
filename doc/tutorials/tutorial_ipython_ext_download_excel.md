## 问题
日志服务的数据并不要求统一格式，每条日志可以有不同的关键字集合，例如:
```json
{"city": "123", "province": "vvv"}
{"city": "shanghai", "pop": "2000"}
{"name": "xiao ming", "home": "shanghai"}
```
因此一般使用日志服务的CLI下载的命令[get_log_all](https://yq.aliyun.com/articles/415489)或者[pull_log_dump](https://yq.aliyun.com/articles/400630)时，格式都是单行JSON格式以保证灵活性。

但是大部分情况下，一个日志库的所有日志的关键字集合总体是稳定的；另一方面，**Excel**格式（或者更简单的**CSV**格式）相对**JSON**更加商业应用和人类操作友好一些。

**如果期望下载下来时是Excel或者CSV格式，并且自动处理字段不一致的情况的话，该怎么办？**

本文通过使用日志服务IPython/Jupyter扩展，轻松做到这点。

## 前提
### 安装日志服务扩展
首先，参考文章[日志服务IPythonIPython/Jupyter扩展](https://yq.aliyun.com/articles/689911#6)完成安装（IPython Shell、IPython/Jupyter Notebook或者Jupyter Lab均可）

### 安装Excel相关组件
在IPython所在环境中安装Excel读写的相关组件：

```shell
pip install openpyxl xlrd xlwt XlsxWriter
```
- openpyxl - 用于Excel 2010 xlsx/xlsm文件的读写
- xlrd - 读取Exce (xls格式）
- xlwt - 写Excel (xls格式）
- XlsxWriter - 写Excel (xlsx)文件

### 配置
使用`%manage_log`配置好链接日志服务的相关入口、秘钥、项目和日志库等。具体参考[这里](https://yq.aliyun.com/articles/689911#9)。

## 场景
### 1. 将结果保存到Excel中

通过查询命令`%%log`查询得到`Pandas Dataframe`，然后调用`to_excel`即可。

样例：
```python
%%log -1day ~ now
* | select date_format(date_trunc('hour', __time__), '%H:%i') as dt,
        count(1)%100 as pv,
        round(sum(if(status < 400, 1, 0))*100.0/count(1), 1) AS ratio
        group by date_trunc('hour', __time__)
        order by dt limit 1000
```

```python
df1 = log_df
df1.to_excel('output.xlsx')
```

### 2. 将结果保存到Excel多个Sheet中
通过`%log`或`%%log`获得多个数据存在不同的Dataframe中后，如下样例操作：

```python
import pandas as pd
writer = pd.ExcelWriter('output2.xlsx')

df1.to_excel(writer, sheet_name='data1')
df2.to_excel(writer, sheet_name='data2')

writer.save()
```


### 3. 定制Excel细节格式

Pandas默认使用`Xlwt模块`写`xls`文件、使用`Openpyxl模块`写`xlsx`文件。而使用`XlsxWriter`写`xlsx`功能更加全面灵活，但需要如下配置。

例如上面例子中的`ExcelWriter`构造时，增加参数即可：
```python
writer = pd.ExcelWriter('output2.xlsx', engine='xlsxwriter')
```
可以定制特定列的格式、样式、甚至直接画Excel图表。具体推荐参考[这篇文章](https://xlsxwriter.readthedocs.io/working_with_pandas.html)。

### 4. 其他格式
`Pandas DataFrame`还可以保存其他格式，例如`csv`、`html`等，可以进一步参考[这里](https://pandas.pydata.org/pandas-docs/stable/user_guide/io.html)。

# 进一步参考
- [日志服务IPythonIPython/Jupyter扩展](https://yq.aliyun.com/articles/689911)
- [Using Excel with pandas](https://www.dataquest.io/blog/excel-and-pandas/)
- [Working with Python Pandas and XlsxWriter](https://xlsxwriter.readthedocs.io/working_with_pandas.html)
- [Pandas IO Tools](https://pandas.pydata.org/pandas-docs/stable/user_guide/io.html)