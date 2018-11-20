from .config_parser import ConfigParser
import os
import sys
import copy
import inspect
import logging
from .exceptions import SettingError
logger = logging.getLogger(__name__)


class Runner(object):
    def __init__(self, config_path):
        if not inspect.ismodule(config_path):
            basedir = os.path.dirname(os.path.abspath(config_path))
            module_name = os.path.basename(config_path[:-3])
            if basedir not in sys.path:
                sys.path.insert(0, basedir)
            try:
                md = __import__(module_name)
            except ImportError as ex:
                logger.error("Cannot import config path: {0}".format(config_path))
                raise SettingError(ex, 'Cannot import the config "{0}"'.format(config_path))
        else:
            md = config_path

        logger.info("runner: passed module {0} from config file {1}".format(md, config_path))

        parsed_fn = ConfigParser(md).parse()
        logger.info("runner: passed fn list: {0}".format(parsed_fn))

        self.fn_list = [fn for no, fn in parsed_fn]

    def __call__(self, event):
        ret = copy.copy(event)
        for fn in self.fn_list:
            ret = fn(ret)
            if ret is None:
                return None

        return ret
