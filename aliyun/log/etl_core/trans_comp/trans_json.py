from .trans_base import trans_comp_base
import six
import re
import logging
import json
from ..etl_util import cached
from collections import Iterable
import jmespath
from jmespath.exceptions import ParseError
from ..exceptions import SettingError
import inspect
from ..etl_util import get_re_full_match

__all__ = ['trans_comp_json']

logger = logging.getLogger(__name__)


def trans_comp_json(*args, **kwargs):
    if (args and isinstance(args[0], dict)) or ('event' in kwargs and isinstance(kwargs['event'], dict)):
        # event case
        trans = json_transformer()
        return trans(*args, **kwargs)
    else:
        return json_transformer(*args, **kwargs)


class json_transformer(trans_comp_base):
    DEFAULT_SEP = '.'
    DEFAULT_FMT = "simple"
    DEFAULT_DEPTH = 100
    DEFAULT_INCLUDE_KEYS = ''
    DEFAULT_EXCLUDE_KEYS = ''

    FMT_MAP = {
        "simple": "{prefix}{current}{suffix}",
        "full":  "{parent_list_str}{sep}{prefix}{current}{suffix}",  # "{sep.join(parent_path)}{sep}{prefix}{current}{suffix}",
        "parent": "{parent}{sep}{prefix}{current}{suffix}",
        "root": "{parent_list[0]}{sep}{prefix}{current}{suffix}"
        # could also be custom formatting string using up to five placehodler: parent_list, current, sep, prefix, suffix
        # could also be formatting function accepting the two parameters: parrent_list, current key, value
        #    Note: the functoin result k, v tuple, if returning None, meaning skip this k-v
    }

    def __init__(self, jmes=None, jmes_ignore_none=None, output=None,
                 expand=None, expand_array=False, depth=None, include_keys=None, exclude_keys=None,
                 fmt=None, sep=None, prefix=None, suffix=None):
        """
        :param jmes:  jmes filter to select or generate new field
        :param jmes_ignore_none: if jmes filter is null, ignore it (default). Or else consider it as "". default is
        :param output: put the value parsed from jmes filter to this field
        :param expand: If jmes filter is configure, expand the result or not (Default is False in this case), If jmes is not configured, directly expand the field passed or not (Default is True in this case).
        :param expand_array: if expand array or just render it. default is True. item in array will be with name index, e.g. [1,2,3] will be considered as {"0": 1, "1": 2, "2": 3}
        :param depth: depth to scan, 1 means first layer, default is 100.
        :param include_keys: keys to expand and include. regex string. using '|' for multiple ones. default is all.
        :param exclude_keys: keys to skip, regex string. using '|' for multiple ones. default is nothing.
        :param fmt: during expansion, how to format the key name, there're several types or customized as described in FMT_MAP
        :param sep: sep used in formatting during expansion
        :param prefix: prefix used in formatting during expansion
        :param suffix: suffix used in formatting during expansion
        """
        self.expand = expand
        if expand is None:
            self.expand = not jmes

        self.expand_array = True if expand_array is None else expand_array
        # self.level = level or 1
        self.jmes = jmes or ""
        self.prefix = "" if prefix is None else prefix
        self.suffix = "" if suffix is None else suffix
        self.sep = self.DEFAULT_SEP if sep is None else sep
        self.output = output or ""
        self.jmes_filter = None
        self.jmes_ignore_none = True if jmes_ignore_none is None else jmes_ignore_none
        if jmes:
            try:
                self.jmes_filter = jmespath.compile(jmes)
            except jmespath.exceptions.ParseError as ex:
                raise SettingError(ex=ex, msg="Invalid JMES filter setting", settings=jmes)
        elif self.output:
            logger.warning("json_transformer: parameter output '{0}' will be ignored as there's no filter is selected."
                        .format(output))

        self.depth = min( (depth or self.DEFAULT_DEPTH), self.DEFAULT_DEPTH)
        self.include_keys = include_keys or self.DEFAULT_INCLUDE_KEYS
        self.exclude_keys = exclude_keys or self.DEFAULT_EXCLUDE_KEYS
        self.fmt = fmt or self.DEFAULT_FMT
        self.include_match = get_re_full_match(self.include_keys)
        self.exclude_match = get_re_full_match(self.exclude_keys)

    @staticmethod
    def format_add_kv(event, fmt, current, value, parent_list, sep, prefix, suffix):
        ret = None
        if isinstance(fmt, (six.text_type, six.binary_type)):
            fmt = json_transformer.FMT_MAP.get(fmt.strip().lower(), fmt)
            try:
                ret = fmt.format(parent=parent_list[-1], parent_list_str=sep.join(parent_list), parent_list=parent_list, current=current, sep=sep, prefix=prefix, suffix=suffix), \
                   json_transformer._n(value)
            except Exception as ex:
                logger.info("json_transformer: fail to format with settings: '{0}'".format( (fmt, current, value,
                                                                                             parent_list, sep, prefix, suffix) ))
        elif isinstance(fmt, inspect.isfunction):
            ret = fmt(parent_list, current, value)

        if ret and len(ret) == 2:
            k, v = ret
            event[json_transformer._n(k)] = json_transformer._n(v)
        else:
            logger.info("json_transformer: unexpected format result: {0}, fmt: '{1}', k: '{2}', v: '{3}', skip it"
                        .format(ret, fmt, current, value))

    def _expand_json(self, event, key, value, parent_list, depth, sep, prefix, suffix):
        # check if need to skip it
        if (self.include_keys and not self.include_match(key)) or (self.exclude_keys and self.exclude_match(key)):
            logger.info("json_transformer: 'key' {0} is not in include keys '{1}' or in exclude keys '{2}', skip it."
                        .format(key, self.include_keys, self.exclude_keys))
            return

        # check if need to format it directly
        if len(parent_list) > depth \
                or (not isinstance(value, (list, tuple, dict))) \
                or (isinstance(value, (list, tuple)) and not self.expand_array):
            # 1. depth hit, 2. basic type, 3. array but not expand
            logger.info("json_transformer: hit stop parsing, key: '{0}', value: '{1}', parent: '{2}', depth: '{3}'"
                        .format(key, value, parent_list, depth))
            self.format_add_kv(event, self.fmt, self._n(key), self._n(value), parent_list, sep, prefix, suffix)
            return None

        # convert array to dict
        if isinstance(value, (list, tuple)):
            value = dict((str(i), v) for i, v in enumerate(value))

        if isinstance(value, dict):
            for k, v in six.iteritems(value):
                if isinstance(v, (dict, tuple, list)):
                    # recursively parse it
                    self._expand_json(event, k, v, parent_list + (k, ), depth, sep, prefix, suffix)
                else:
                    self.format_add_kv(event, self.fmt, self._n(k), self._n(v), parent_list, sep, prefix, suffix)

        else:
            logger.info("json_transformer: skip unsupported message '{0}' of type '{1}' when expanding"
                        .format(value, type(value)))

    def _process_message(self, key, value):
        new_event = {}
        if isinstance(value, (six.binary_type, six.text_type)):
            try:
                value = json.loads(value)
            except Exception as ex:
                logger.info("json_transformer: fail to load event into json object: {0}, error: {1}".format(value, ex))
                return None

        if self.jmes_filter:
            try:
                value = self.jmes_filter.search(value)
                if value is None and self.jmes_ignore_none:
                    logger.info("split_event_transformer: value {0} get null from jmes settings {1}, skip it".
                                format(value, self.jmes))
                    return None
            except Exception as ex:
                logger.info("split_event_transformer: value {0} with invalid jmes settings {1}, skip it".
                            format(value, self.jmes))
                return None

            if self.output:
                new_event[self.output] = self._n(value)

        # if need to expand
        if self.expand:
            self._expand_json(new_event, key, value, (key, ), self.depth, self.sep, self.prefix, self.suffix)

        return new_event

    def extract_json(self, message):
        new_event = {}
        if isinstance(message, (six.binary_type, six.text_type)):
            try:
                message = json.loads(message)
            except Exception as ex:
                logger.info("json_transformer: fail to load event into json object: {0}, error: {1}".format(message, ex))
                return message

        if isinstance(message, dict):
            for k, v in six.iteritems(message):
                new_event["{0}{1}{2}".format(self.prefix, self._n(k), self.suffix)] = self._n(v)

        return new_event

    def __call__(self, event, inpt):
        # simple dict mode
        if isinstance(inpt, (six.binary_type, six.text_type)):
            inpt = [inpt]

        if isinstance(inpt, Iterable):
            for i in inpt:
                if not isinstance(i, (six.binary_type, six.text_type)):
                    logger.error('trans_comp_lookup: type of input field "{0}" is unknown'.format(i))
                    continue

                if i not in event:
                    logger.info('trans_comp_lookup: event "{0}" does not contain field "{1}"'.format(event, i))
                    continue

                # get input value
                new_event = self._process_message(i, event[i])
                if new_event and isinstance(new_event, dict):
                    event.update(new_event)
                else:
                    logger.info('trans_comp_lookup: event "{0}" does not extract value from field "{1}"'.format(event, i))
        else:
            logger.error("trans_comp_lookup: unknown type of input field {0}".format(inpt))

        return event
