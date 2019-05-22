import inspect
import json
import logging
try:
    from collections.abc import Iterable
except ImportError:
    from collections import Iterable

import jmespath
import re
import six
from jmespath.exceptions import ParseError

from .trans_base import trans_comp_check_mdoe_base
from ..etl_util import get_re_full_match
from ..exceptions import SettingError

__all__ = ['trans_comp_json']

logger = logging.getLogger(__name__)


def trans_comp_json(*args, **kwargs):
    if (args and isinstance(args[0], dict)) or ('event' in kwargs and isinstance(kwargs['event'], dict)):
        # event case
        trans = json_transformer()
        return trans(*args, **kwargs)
    else:
        return json_transformer(*args, **kwargs)


class json_transformer(trans_comp_check_mdoe_base):
    DEFAULT_SEP = u'.'
    DEFAULT_FMT = "simple"
    DEFAULT_DEPTH = 100
    DEFAULT_INCLUDE_NODE = trans_comp_check_mdoe_base.DEFAULT_KEYWORD_PTN
    DEFAULT_EXCLUDE_NODE = u''
    DEFAULT_INCLUDE_PATH = u''
    DEFAULT_EXCLUDE_PATH = u''

    DEFAULT_FMT_ARRAY = u"{parent_rlist[0]}_{index}"  # could also be custom formatting string using up to five placehodler: parent_list, parent_list, current, sep, prefix, suffix
    FMT_MAP = {
        "simple": lambda prefix, current, suffix, *args, **kwargs: u"{prefix}{current}{suffix}".format(prefix=prefix,
                                                                                                      current=current,
                                                                                                      suffix=suffix),
        "full": lambda parent_list, sep, prefix, current, suffix, *args,
                       **kwargs: u"{parent_list_str}{sep}{prefix}{current}{suffix}".format(
            parent_list_str=sep.join(parent_list), current=current, sep=sep, prefix=prefix, suffix=suffix),
        "parent": lambda parent_list, sep, prefix, current, suffix, *args,
                         **kwargs: u"{parent}{sep}{prefix}{current}{suffix}".format(parent=parent_list[-1],
                                                                                   current=current, sep=sep,
                                                                                   prefix=prefix, suffix=suffix),
        "root": lambda parent_list, sep, prefix, current, suffix, *args,
                       **kwargs: u"{parent_list[0]}{sep}{prefix}{current}{suffix}".format(parent_list=parent_list,
                                                                                         current=current, sep=sep,
                                                                                         prefix=prefix, suffix=suffix)
        # could also be custom formatting string using up to five placehodler: parent_list, parent_list, current, sep, prefix, suffix
        # could also be formatting function accepting the 3 parameters: parrent_list, current key, current value
        #    Note: the functoin must result k, v tuple, if returning None, meaning skip this k-v
    }

    def __init__(self, jmes=None, jmes_ignore_none=None, output=None,
                 expand=None, depth=None, include_node=None, exclude_node=None,
                 include_path=None, exclude_path=None,
                 fmt=None, sep=None, prefix=None, suffix=None,
                 expand_array=None, fmt_array=None, mode=None
                 ):
        """
        :param jmes:  jmes filter to select or generate new field
        :param jmes_ignore_none: if jmes filter is null, ignore it (default). Or else consider it as "". default is
        :param output: put the value parsed from jmes filter to this field
        :param expand: If jmes filter is configure, expand the result or not (Default is False in this case), If jmes is not configured, directly expand the field passed or not (Default is True in this case).
        :param depth: depth to scan, 1 means first layer, default is 100.
        :param include_node: keys to expand and include. regex string. using '|' for multiple ones. default is all.
        :param exclude_node: keys to skip, regex string. using '|' for multiple ones. default is nothing.
        :param include_path: path to expand and include. regex string to match from begin. using '|' for multiple ones. default is all. e.g. r"data\.k1", all sub-keys in data.k1 will be included.
        :param exclude_path: path to skip, regex string to match from begin. using '|' for multiple ones. default is nothing. . e.g. r"data\.k2", all sub-keys in data.k2 will be excluded.
        :param fmt: during expansion, how to format the key name, there're several types or customized as described in FMT_MAP
        :param sep: sep used in formatting during expansion
        :param prefix: prefix used in formatting during expansion
        :param suffix: suffix used in formatting during expansion
        :param expand_array: if expand array or just render it. default is True. item in array will be with name index, e.g. [1,2,3] will be considered as {"0": 1, "1": 2, "2": 3}
        :param fmt_array: format string for key name of each array element, default is "{parent_rlist[0]}_{index}", can be custom formatting string using placehodler: parent_list, parent_list, current
        """
        super(json_transformer, self).__init__(mode=mode)

        self.expand = expand
        if expand is None:
            # when jmes is not configured or configure but no output configured
            self.expand = not jmes or not output

        # self.level = level or 1
        self.jmes = self._u(jmes or "")
        self.prefix = self._u("" if prefix is None else prefix)
        self.suffix = self._u("" if suffix is None else suffix)
        self.sep = self._u(self.DEFAULT_SEP if sep is None else sep)
        self.output = self._u(output or "")
        self.jmes_filter = None
        self.jmes_ignore_none = True if jmes_ignore_none is None else jmes_ignore_none
        if jmes:
            try:
                self.jmes_filter = jmespath.compile(jmes)
            except jmespath.exceptions.ParseError as ex:
                raise SettingError(ex=ex, msg="Invalid JMES filter setting", settings=jmes)
        elif self.output:
            logger.warning(u"json_transformer: parameter output '{0}' will be ignored as there's no filter is selected."
                           .format(output))

        self.depth = min((depth or self.DEFAULT_DEPTH), self.DEFAULT_DEPTH)
        self.include_node = self._u(include_node or self.DEFAULT_INCLUDE_NODE)
        self.exclude_node = self._u(exclude_node or self.DEFAULT_EXCLUDE_NODE)
        self.include_path = self._u(include_path or self.DEFAULT_INCLUDE_PATH)
        self.exclude_path = self._u(exclude_path or self.DEFAULT_EXCLUDE_PATH)
        self.fmt = self._u(fmt or self.DEFAULT_FMT)

        try:
            self.include_node_match = get_re_full_match(self.include_node)
            self.exclude_node_match = get_re_full_match(self.exclude_node)
            self.include_path_match = re.compile(self.include_path).match  # use match instead of full match
            self.exclude_path_match = re.compile(self.exclude_path).match  # use match instead of full match
        except Exception as ex:
            raise SettingError(ex=ex, msg="Invalid regex string for include/exclude")

        self.expand_array = True if expand_array is None else expand_array
        self.format_array = self._u(fmt_array or self.DEFAULT_FMT_ARRAY)

    def _skip_keys(self, key, parent_list):
        if (self.include_node and not self.include_node_match(key)) or (
                self.exclude_node and self.exclude_node_match(key)):
            logger.info(u"json_transformer: 'key' {0} is not in include keys '{1}' or in exclude keys '{2}', skip it."
                        .format(key, self.include_node, self.exclude_node))
            return True

        if self.include_path or self.exclude_path:
            path = '.'.join(parent_list) + '.' + key
            if (self.include_path and not self.include_path_match(path)) or (
                    self.exclude_path and self.exclude_path_match(path)):
                logger.info(
                    u"json_transformer: path '{0}' is not in include path '{1}' or in exclude path '{2}', skip it."
                    .format(path, self.include_path, self.exclude_path))
                return True

        return False

    def format_add_kv(self, event, fmt, current, value, parent_list, parent_rlist, sep, prefix, suffix):
        if self._skip_keys(current, parent_list):
            logger.info(u"json_transformer: 'key' {0} is not in include keys '{1}' or in exclude keys '{2}', skip it."
                        .format(current, self.include_node, self.exclude_node))
            return

        ret = None
        if isinstance(fmt, (six.text_type, six.binary_type)):
            fmt = json_transformer.FMT_MAP.get(fmt.strip().lower(), fmt)
            try:
                if isinstance(fmt, (six.text_type, six.binary_type)):
                    ret = fmt.format(parent_list=parent_list, parent_rlist=parent_rlist, current=current, sep=sep,
                                     prefix=prefix, suffix=suffix), \
                          json_transformer._n(value)
                else:
                    # callable formatting function
                    ret = fmt(parent_list=parent_list, parent_rlist=parent_rlist, current=current, sep=sep,
                              prefix=prefix, suffix=suffix), \
                          json_transformer._n(value)
            except Exception as ex:
                logger.info(u"json_transformer: fail to format with settings: '{0}'".format((fmt, current, value,
                                                                                            parent_list, sep, prefix,
                                                                                            suffix)))
        elif inspect.isfunction(fmt):
            try:
                ret = fmt(parent_list, current, value)
            except Exception as ex:
                logger.info(u"json_transformer: fail to call formatting string: {0} wuth parameters: {1}"
                            .format(fmt, (parent_list, current, value)))

        if ret and len(ret) == 2:
            k, v = ret
            event[json_transformer._n(k)] = json_transformer._n(v)
        else:
            logger.info(u"json_transformer: unexpected format result: {0}, fmt: '{1}', k: '{2}', v: '{3}', skip it"
                        .format(ret, fmt, current, value))

    def _expand_json(self, event, key, value, parent_list, parent_rlist, depth, sep, prefix, suffix):
        # check if need to format it directly
        if len(parent_list) > depth \
                or (not isinstance(value, (list, tuple, dict))) \
                or (isinstance(value, (list, tuple)) and not self.expand_array):
            # 1. depth hit, 2. basic type, 3. array but not expand
            logger.info(u"json_transformer: hit stop parsing, key: '{0}', value: '{1}', parent: '{2}', depth: '{3}'"
                        .format(key, value, parent_list, depth))
            self.format_add_kv(event, self.fmt, self._n(key), self._n(value), parent_list, parent_rlist, sep, prefix,
                               suffix)
            return None

        # convert array to dict
        if isinstance(value, (list, tuple)):
            value = dict(
                (self.format_array.format(parent_list=parent_list, parent_rlist=parent_rlist, index=i), v) for i, v in
                enumerate(value))

        if isinstance(value, dict):
            for k, v in six.iteritems(value):
                if isinstance(v, (dict, tuple, list)):
                    # recursively parse it
                    self._expand_json(event, k, v, parent_list + (k,), (k,) + parent_rlist, depth, sep, prefix, suffix)
                else:
                    self.format_add_kv(event, self.fmt, self._n(k), self._n(v), parent_list, parent_rlist, sep, prefix,
                                       suffix)

        else:
            logger.info(u"json_transformer: skip unsupported message '{0}' of type '{1}' when expanding"
                        .format(value, type(value)))

    def _process_message(self, key, value):
        new_event = {}
        if isinstance(value, (six.binary_type, six.text_type)):
            try:
                value = json.loads(value)
            except Exception as ex:
                logger.info(u"json_transformer: fail to load event into json object: {0}, error: {1}".format(value, ex))
                return None

        if self.jmes_filter:
            try:
                value = self.jmes_filter.search(value)
                if value is None and self.jmes_ignore_none:
                    logger.info(u"split_event_transformer: value {0} get null from jmes settings {1}, skip it".
                                format(value, self.jmes))
                    return None
            except Exception as ex:
                logger.info(u"split_event_transformer: value {0} with invalid jmes settings {1}, skip it".
                            format(value, self.jmes))
                return None

            if self.output:
                new_event[self.output] = self._n(value)

        # if need to expand
        if self.expand:
            self._expand_json(new_event, key, value, (key,), (key,), self.depth, self.sep, self.prefix, self.suffix)

        return new_event

    def __call__(self, event, inpt):
        inpt = self._u(inpt)

        # simple dict mode
        if isinstance(inpt, (six.binary_type, six.text_type)):
            inpt = [inpt]

        if isinstance(inpt, Iterable):
            for i in inpt:
                if not isinstance(i, (six.binary_type, six.text_type)):
                    logger.error(u'trans_comp_lookup: type of input field "{0}" is unknown'.format(i))
                    continue

                if i not in event:
                    logger.info(u'trans_comp_lookup: event "{0}" does not contain field "{1}"'.format(event, i))
                    continue

                # get input value
                new_event = self._process_message(i, event[i])
                if new_event and isinstance(new_event, dict):
                    #event.update(new_event)
                    self.sets(event, new_event)
                else:
                    logger.info(
                        u'trans_comp_lookup: event "{0}" does not extract value from field "{1}"'.format(event, i))
        else:
            logger.error(u"trans_comp_lookup: unknown type of input field {0}".format(inpt))

        return event
