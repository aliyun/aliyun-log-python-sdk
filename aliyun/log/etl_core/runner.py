import copy
import inspect
import logging
import os
import sys

from .config_parser import ConfigParser
from .exceptions import SettingError
from .etl_util import process_event, u

logger = logging.getLogger(__name__)


class Runner(object):
    def __init__(self, config_path):
        config_path = u(config_path)

        if not inspect.ismodule(config_path):
            basedir = os.path.dirname(os.path.abspath(config_path))
            module_name = os.path.basename(config_path[:-3])
            if basedir not in sys.path:
                sys.path.insert(0, basedir)
            try:
                md = __import__(module_name)
            except ImportError as ex:
                logger.error(u"Cannot import config path: {0}".format(config_path))
                raise SettingError(ex, u'Cannot import the config "{0}"'.format(config_path))
        else:
            md = config_path

        logger.info(u"runner: passed module {0} from config file {1}".format(md, config_path))

        parsed_fn = ConfigParser(md).parse()
        logger.info(u"runner: passed fn list: {0}".format(parsed_fn))

        self.fn_list = [fn for no, fn in parsed_fn]

    def __call__(self, event):
        return process_event(event, self.fn_list)
