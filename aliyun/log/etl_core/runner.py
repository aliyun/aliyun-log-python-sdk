import copy
import inspect
import logging
import os
import sys

from .config_parser import ConfigParser
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

    def _process_event(self, event, fn_list):
        if not len(fn_list):
            return event

        new_event = copy.copy(event)
        for i, fn in enumerate(fn_list):
            new_event = fn(new_event)
            if new_event is None:
                return None

            if isinstance(new_event, (tuple, list)):
                result = []
                for e in new_event:
                    ret = self._process_event(e, fn_list[i + 1:])
                    if ret is None:
                        continue

                    if isinstance(ret, (tuple, list)):
                        result.extend(ret)
                    else:
                        result.append(ret)

                if result:
                    if len(result) == 1:
                        return result[0]
                    return result
                return None  # return None for empty list

        return new_event

    def __call__(self, event):
        return self._process_event(event, self.fn_list)
