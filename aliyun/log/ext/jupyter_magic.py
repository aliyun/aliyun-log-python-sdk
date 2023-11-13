# encoding: utf8

from IPython.core.magic import (Magics, magics_class, line_magic,
                                cell_magic, line_cell_magic)
import pandas as pd
from IPython.display import display, clear_output
import re, time, threading, datetime
from pandas import DataFrame
from aliyun.log import LogClient, LogException
from concurrent.futures import ThreadPoolExecutor as PoolExecutor, as_completed
import multiprocessing
import six
import six.moves.configparser as configparser
import os
import sys


def can_use_widgets():
    """ Expanded from from http://stackoverflow.com/a/34092072/1958900
    """
    if 'IPython' not in sys.modules:
        # IPython hasn't been imported, definitely not
        return False
    from IPython import get_ipython

    # check for `kernel` attribute on the IPython instance
    if getattr(get_ipython(), 'kernel', None) is None:
        return False

    try:
        import ipywidgets as ipy
        import traitlets
    except ImportError:
        return False

    if int(ipy.__version__.split('.')[0]) < 6:
        print('WARNING: widgets require ipywidgets 6.0 or later')
        return False

    return True

__CAN_USE_WIDGET__ = can_use_widgets()


CLI_CONFIG_FILENAME = "%s/.aliyunlogcli" % os.path.expanduser('~')
MAGIC_SECTION = "__jupyter_magic__"
DEFAULT_DF_NAME = 'log_df'
DEFAULT_TMP_DF_NAME = 'log_df_part'

result = None
detail = None


def _load_config():
    global g_default_region, g_default_ak_id,  g_default_ak_key, g_default_project, g_default_logstore

    def _get_section_option(config, section_name, option_name, default=None):
        if six.PY3:
            return config.get(section_name, option_name, fallback=default)
        else:
            return config.get(section_name, option_name) if config.has_option(section_name, option_name) else default

    config = configparser.ConfigParser()
    config.read(CLI_CONFIG_FILENAME)
    g_default_region = _get_section_option(config, MAGIC_SECTION, "region-endpoint", "")
    g_default_ak_id = _get_section_option(config, MAGIC_SECTION, "access-id", "")
    g_default_ak_key = _get_section_option(config, MAGIC_SECTION, "access-key", "")
    g_default_project = _get_section_option(config, MAGIC_SECTION, "project", "")
    g_default_logstore = _get_section_option(config, MAGIC_SECTION, "logstore", "")


def _save_config(region, ak_id, ak_key, project, logstore):
    global g_default_region, g_default_ak_id, g_default_ak_key, g_default_project, g_default_logstore

    config = configparser.ConfigParser()
    config.read(CLI_CONFIG_FILENAME)

    if not config.has_section(MAGIC_SECTION):
        config.add_section(MAGIC_SECTION)

    config.set(MAGIC_SECTION, "region-endpoint", region)
    config.set(MAGIC_SECTION, "access-id", ak_id)
    config.set(MAGIC_SECTION, "access-key", ak_key)
    config.set(MAGIC_SECTION, "project", project)
    config.set(MAGIC_SECTION, "logstore", logstore)

    # save to disk
    with open(CLI_CONFIG_FILENAME, 'w') as configfile:
        config.write(configfile)

    # save to memory
    g_default_region, g_default_ak_id, g_default_ak_key, g_default_project, g_default_logstore = region, ak_id, ak_key, project, logstore

_load_config()


def parse_timestamp(tm):
    return datetime.datetime.fromtimestamp(int(tm)).isoformat()


@magics_class
class MyMagics(Magics):
    logclient = None

    @staticmethod
    def pull_worker(client, project_name, logstore_name, from_time, to_time, shard_id):
        res = client.pull_log(project_name, logstore_name, shard_id, from_time, to_time)
        result = []
        next_cursor = 'as from_time configured'
        try:
            for data in res:
                result.extend(data.get_flatten_logs_json(decode_bytes=True))
                next_cursor = data.next_cursor
        except Exception as ex:
            print("dump log failed: task info {0} failed to copy data to target, next cursor: {1} detail: {2}".
                format(
                (project_name, logstore_name, shard_id, from_time, to_time),
                next_cursor, ex))

        return result

    @staticmethod
    def pull_log_all(client, project_name, logstore_name, from_time, to_time):
        cpu_count = multiprocessing.cpu_count() * 2

        shards = client.list_shards(project_name, logstore_name).get_shards_info()
        current_shards = [str(shard['shardID']) for shard in shards]
        target_shards = current_shards
        worker_size = min(cpu_count, len(target_shards))

        result = []
        with PoolExecutor(max_workers=worker_size) as pool:
            futures = [pool.submit(MyMagics.pull_worker, client, project_name, logstore_name, from_time, to_time,
                                   shard_id=shard)
                       for shard in target_shards]

            try:
                for future in as_completed(futures):
                    data = future.result()
                    result.extend(data)

                return True, result
            except KeyboardInterrupt as ex:
                clear_output()
                print(u"正在取消当前获取……")
                for future in futures:
                    if not future.done():
                        future.cancel()

        return False, result

    def client(self, reset=False):
        if self.logclient is None or reset:
            self.logclient = LogClient(g_default_region, g_default_ak_id, g_default_ak_key)
        return self.logclient

    def verify_sls_connection(self, region, ak_id, ak_key, project, logstore):
        logclient = LogClient(region, ak_id, ak_key)
        try:
            res = logclient.get_logstore(project, logstore)
            return True, res.body
        except LogException as ex:
            return False, str(ex)
        except Exception as ex:
            return False, str(ex)

    @staticmethod
    def _get_log_param(line, cell):
        to_time = time.time()
        from_time = to_time - 60*15
        if cell is None:
            query = line
        else:
            line = line.strip()
            ret = [line, '']
            if '~' in line:
                ret = line.split('~')
            elif '-' in line:
                ret = line.split('~')

            from_time = ret[0].strip() or from_time
            to_time = ret[1].strip() or to_time if len(ret) > 1 else to_time

            query = cell

        return from_time, to_time, query

    def log_imp(self, line, cell=None):
        from_time, to_time, query = self._get_log_param(line, cell)

        print(u"根据查询统计语句，从日志服务查询数据(时间范围：{0} ~ {1})，结果将保存到变量{2}中，请稍等……".format(from_time, to_time, DEFAULT_DF_NAME))
        res = self.client().get_log_all(g_default_project, g_default_logstore, from_time=from_time, to_time=to_time,
                                         query=query)
        is_complete = True
        logs = []
        try:
            for data in res:
                if not data.is_completed():
                    is_complete = False
                logs.extend(data.body)
        except Exception as ex:
            print(ex)
            return

        df1 = pd.DataFrame(logs)
        is_stat = re.match(r'.+\|\s+select\s.+|^\s*select\s+.+', query.lower(), re.I) is not None
        if is_stat:
            if "__time__" in df1:
                del df1["__time__"]
            if "__source__" in df1:
                del df1["__source__"]
        else:
            # change time to date time
            if "__time__" in df1:
                df1['__time__'] = pd.to_datetime(df1["__time__"].apply(parse_timestamp))
                df1.set_index('__time__', inplace=True)

        clear_output()
        if is_complete:
            print(u"变量名：{0}".format(DEFAULT_DF_NAME))
        elif is_stat:
            print(u"变量名：{0}，结果非完整精确结果。".format(DEFAULT_DF_NAME))
        else:
            print(u"变量名：{0}，部分结果非完整精确结果。".format(DEFAULT_DF_NAME))

        self.shell.user_ns[DEFAULT_DF_NAME] = df1
        return df1

    def fetch_log_imp(self, line):
        from_time, to_time, _ = self._get_log_param(line, "")

        print(u"从日志服务拉取数据(日志插入时间：{0} ~ {1})，结果将保存到变量{2}中，请稍等……".format(from_time, to_time, DEFAULT_DF_NAME))
        result, logs = self.pull_log_all(self.client(), g_default_project, g_default_logstore, from_time, to_time)

        df1 = pd.DataFrame(logs)

        # change time to date time
        if "__time__" in df1:
            df1['__time__'] = pd.to_datetime(df1["__time__"].apply(parse_timestamp))
            df1.set_index('__time__', inplace=True)

        clear_output()
        if not result:
            print(u"获取被取消，显示部分结果，变量名：{0}".format(DEFAULT_TMP_DF_NAME))
            self.shell.user_ns[DEFAULT_TMP_DF_NAME] = df1
        else:
            print(u"变量名：{0}".format(DEFAULT_DF_NAME))
            self.shell.user_ns[DEFAULT_DF_NAME] = df1
        return df1

    @line_cell_magic
    def log(self, line, cell=None):
        return self.log_imp(line, cell)

    @line_cell_magic
    def fetch(self, line):
        return self.fetch_log_imp(line)

    @line_magic
    def manage_log(self, line):
        line = line or ""
        if line or not __CAN_USE_WIDGET__:
            params = line.split(" ")
            if len(params) == 5:
                print(u"连接中...")
                endpoint, key_id, key_val, project, logstore = params
                result, detail = self.verify_sls_connection(endpoint, key_id, key_val, project, logstore)

                if result:
                    clear_output()
                    print(u"连接成功.")
                    _save_config(endpoint, key_id, key_val, project, logstore)
                    self.client(reset=True)
                else:
                    print(detail)
            else:
                print(u"参数错误，请使用GUI配置（无参）或遵循格式：%manage_log <endpoint> <ak_id> <ak_key> <project> <logstore>")

            return

        import ipywidgets as widgets
        w_1 = widgets.ToggleButtons( options=[u'基本配置', u"高级配置"] )

        w_endpoint = widgets.Text( description=u'服务入口', value=g_default_region)
        w_key_id = widgets.Password( description=u'秘钥ID', value=g_default_ak_id)
        w_key_val = widgets.Password( description=u'秘钥Key', value=g_default_ak_key)
        w_project = widgets.Text( description=u'默认项目', value=g_default_project)
        w_logstore = widgets.Text( description=u'默认日志库', value=g_default_logstore)
        w_confirm = widgets.Button(
            description=u'修改' if g_default_region else u'确认',
            button_style='info',
            icon='confirm'
        )
        w_result = widgets.Label(value='')
        hide_layout = widgets.Layout(height="0px")
        show_layout = widgets.Layout(height='auto')
        progress = widgets.FloatProgress(description="", value=0.0, min=0.0, max=1.0, layout=hide_layout)

        def work(progress):
            global result
            total = 100
            for i in range(total):
                time.sleep(0.2)
                if result is None:
                    progress.value = float(i+1)/total
                else:
                    progress.value = 100
                    progress.description = u"完成" if result else u"失败"
                    break

        def on_button_clicked(b):
            global result, detail
            progress.layout = show_layout
            progress.description = u"连接中..."
            progress.value = 0
            w_result.value = ""

            result = None
            detail = ""
            thread = threading.Thread(target=work, args=(progress,))
            thread.start()

            result, detail = self.verify_sls_connection(w_endpoint.value, w_key_id.value, w_key_val.value, w_project.value, w_logstore.value)

            if result:
                w_result.value = u"连接成功."
                _save_config(w_endpoint.value, w_key_id.value, w_key_val.value, w_project.value, w_logstore.value)
                self.client(reset=True)
            else:
                w_result.value = str(detail)

        w_confirm.on_click(on_button_clicked)

        p = widgets.VBox(children=[w_1, w_endpoint, w_key_id, w_key_val, w_project, w_logstore, w_confirm, progress, w_result])

        return p


def df_html(df1):
    if not __CAN_USE_WIDGET__:
        return df1._repr_html_()

    try:
        import odps
        if len(df1.columns) > 1:
            df2 = odps.DataFrame(df1)
            return df2._repr_html_()
        return df1._repr_html_()
    except Exception as ex:
        print(ex)
        return df1._repr_html_()


def load_ipython_extension(ipython):
    ipython.register_magics(MyMagics)
    html_formatter = ipython.display_formatter.formatters['text/html']
    html_formatter.for_type(DataFrame, df_html)
