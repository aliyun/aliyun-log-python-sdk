# encoding: utf-8
from __future__ import print_function

import time

from aliyun.log import *


def main():
    endpoint = 'cn-hangzhou.log.aliyuncs.com'
    access_key_id = ''
    access_key = ''
    project_name = 'test-project'
    logstore_name = 'test_logstore'

    client = LogClient(endpoint, access_key_id, access_key)

    # sample_execute_logstore_sql
    print("===sample_execute_logstore_sql===")
    res = client.execute_logstore_sql(project_name, logstore_name,
                                      int(time.time() - 60), int(time.time()),
                                      "* | select count(1) as cnt", True)
    res.log_print()
    print("processed_rows: %s" % res.get_processed_rows())
    print("elapsed_mills: %s" % res.get_elapsed_mills())
    print("has_sql: %s" % res.get_has_sql())
    print("where_query: %s" % res.get_where_query())
    print("agg_query: %s" % res.get_agg_query())
    print("cpu_sec: %s" % res.get_cpu_sec())
    print("cpu_cores: %s" % res.get_cpu_cores())

    # sample_execute_project_sql
    print("===sample_execute_project_sql===")
    res = client.execute_project_sql(project_name, "select count(1) as cnt from %s where __time__ > %s"
                                     % (logstore_name, int(time.time() - 60)), True)
    res.log_print()
    print("processed_rows: %s" % res.get_processed_rows())
    print("elapsed_mills: %s" % res.get_elapsed_mills())
    print("has_sql: %s" % res.get_has_sql())
    print("where_query: %s" % res.get_where_query())
    print("agg_query: %s" % res.get_agg_query())
    print("cpu_sec: %s" % res.get_cpu_sec())
    print("cpu_cores: %s" % res.get_cpu_cores())

    # sample_create_sql_instance
    print("===sample_create_sql_instance===")
    res = client.create_sql_instance(project_name, 500)
    res.log_print()

    # sample_update_sql_instance
    print("===sample_update_sql_instance===")
    res = client.update_sql_instance(project_name, 800)
    res.log_print()

    # sample_list_sql_instance
    print("===sample_list_sql_instance===")
    res = client.list_sql_instance(project_name)
    res.log_print()


if __name__ == '__main__':
    main()
