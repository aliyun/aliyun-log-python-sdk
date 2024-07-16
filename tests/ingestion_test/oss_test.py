# encoding: utf-8
import json
import os
import time

from aliyun.log import *

# basic settings
endpoint = os.environ.get('ALIYUN_LOG_SAMPLE_ENDPOINT', '')
access_key_id = os.environ.get('ALIYUN_LOG_SAMPLE_ACCESSID', '')
access_key_secret = os.environ.get('ALIYUN_LOG_SAMPLE_ACCESSKEY', '')
project = os.environ.get('ALIYUN_LOG_SAMPLE_PROJECT', '')
logstore = os.environ.get('ALIYUN_LOG_SAMPLE_LOGSTORE', '')

client = LogClient(endpoint, access_key_id, access_key_secret)


def _sample_csv_format():
    return {
        "type": "CSV",             # 数据格式，CSV
        "fieldDelimiter": ",",     # 分隔符
        "quoteChar": "\"",         # 引号
        "escapeChar": "\\",        # 转译符
        "maxLines": 10,            # 日志最大跨行数
        "firstRowAsHeader": True,  # 首行作为字段名称
        "skipLeadingRows": 0       # 跳过行数
    }


def _sample_multiline_format():
    return {
        "type": "Multiline",  # 数据格式，跨行文本日志
        "match": "before",    # 正则匹配位置：before(尾行正则）, after(首行正则)
        "pattern": "test",    # 正则表达式
        "maxLines": 10        # 最大行数
    }


def get_sample_job_config(job_name):
    job = {
        "name": job_name,
        "type": "Ingestion",
        "displayName": job_name,
        "description": "",
        "state": "Enabled",
        "schedule": {
            "type": "Resident",
            "runImmediately": True
        },
        "configuration": {
            "version": "v2.0",
            "logstore": logstore,
            "source": {
                "type": "AliyunOSS",
                "bucket": "ali-oss-demo",  # OSS Bucket名称
                "region": "oss-cn-hangzhou",   # OSS区域，例如:oss-cn-shanghai,oss-cn-hangzhou等
                "prefix": "",                  # 文件前缀过滤（可选）
                "pattern": "",                 # 文件正则过滤（可选）
                # "startTime": 1681632123,   # 起始时间（可选）
                # "endTime": 1682236927,     # 结束时间（可选）
                "format": {
                    # 数据格式，取值：JSON,CSV,Line,Multiline,ORC,Parquet,ossAccessLog,cdnDownloadedLog
                    "type": "JSON"
                    # 如果type为CSV或Multiline，format中还需包含更多参数，参考_sample_csv_format和_sample_multiline_format函数
                },
                "compressionCodec": "none",    # 压缩格式，取值：none,autoDetect,gzip,bzip2,snappy,zip,zstd,deflate,lz4
                "encoding": "UTF-8",           # 编码格式，取值：UTF-8,GBK
                "interval": "30m",             # 检查新文件周期，取值：5m,30m,1h,never
                "restoreObjectEnabled": True,  # 导入归档文件，取值：True,False
                "timeField": "time",           # 时间字段（可选）
                "timePattern": "",             # 提取时间正则（可选）
                "timeFormat": "yyyy-MM-dd HH:mm:ss",  # 时间字段格式（可选），例如：epoch,epochMillis,yyyy-MM-dd HH:mm:ss等
                "timeZone": "GMT+08:00",       # 时间字段时区（可选），例如：GMT+08:00
                "useMetaIndex": False          # 使用OSS元数据索引，取值：True,False
            }
        }
    }
    return json.dumps(job)


def create_job(job_name):
    if not job_name:
        job_name = "ingest-oss-%d-0424" % int(time.time())

    job_config = get_sample_job_config(job_name)
    print("job config: ", job_config)

    client.create_ingestion(project_name=project, ingestion_config=job_config)


def restart_job(job_name):
    job_config = get_sample_job_config(job_name)
    print("job config: ", job_config)

    # update the job and then restart it
    client.restart_ingestion(project_name=project,
                             ingestion_name=job_name, ingestion_config=job_config)


def delete_job(job_name):
    client.delete_ingestion(project_name=project, ingestion_name=job_name)


if __name__ == '__main__':
    create_job(job_name="")
    # restart_job(job_name="ingest-oss-1682244222-0424")
    # delete_job(job_name="ingest-oss-1682244222-0424")
