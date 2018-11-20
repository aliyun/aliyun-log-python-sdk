"""
Transform list:
    v ==> [v]

1. dict
    1. fixed k-v
    dict-string

    2. fixed key - function
    dict-function

    input: event
    result: field value

2. regex
    bi-string tuple

    ("data", r"(?P<f1>...)....(?P<f2>...)")

3. event function

    1. raw
    function

    input: event
    output: replaced_event

    2. drop
    DROP_EVENT(condition_list)
    DROP_FIELDS(condition_list)

    3. keep
    KEEP_EVENT(condition_list)
    KEEP_FIELDS(condition_list)

    4. rename
    FIELDS_RENAME_LIST(condition_list)


4. field function
    1. raw
    ("f1", lambda...)
    (["f1", "f2"], lambda...)

    input: user_input, event
    result: updated event

    2. XSV
    ("input_field", CSV("F1, F2, F3, F4"))
    ("input_field", TSV("F1, F2, F3, F4"))
    ("input_field", CSV(["F1", "F2", "F3", "F4"], SEP="\s*,\s*"))
    ("input_field", CSV(["F1", "F2", "F3", "F4"], SEP=",|\|") )

    3. lookup - dict
    # dct['*'] will match if any
    tuple/ list
    ("f1", LOOKUP({....}, "f2"))

    4. lookup - table
    # dct['*'] will match if any
    (["f1", 'f2', 'f3'], LOOKUP("./data.csv", ['out1', 'data2'])
    ([("f1", "f1_alias"), ("f2", "f2_alias"), 'f3'], LOOKUP("./data.csv", [('out1', 'out1_alias'), 'data2']) )

    5. JSON
    ("f1", JSON(filter="jmes_filter...", output="output_filed"))
    # f1: {"a": 1, "b": 2} =>
    ("f1", JSON(expand=True, level=1, prefix="f1.", suffix="", join="."))

    6. KV
    tuple/ list
    ("f1", KV(prefix="f1.", suffix="", sep="=")

"""

import logging
import re
import six

from .trans_base import trans_comp_base
from ..exceptions import SettingError

logger = logging.getLogger(__name__)

__all__ = ['trans_comp_regex']


class trans_comp_regex(trans_comp_base):
    """
    "123 456": "\d+", "f1" ==> f1: 123
    "123 456": "\d+", ["f1", "f2"] ==> f1: 123   f2: 456
    "abc 123": "(\w+) (\d+)", ["f1", "f2"]  ==> f1: abc   f2: 456
    "abc 123": "(?P<f1>\w+) (?P<f2>\d+)" ==> f1: abc   f2: 456

    "abc:123 xyz:456": "(\w+):(\d+)", {"k_\1": "v_\2"} ==> k_abc: 123    k_xyz: 456

    Corner Case:
    "!abc 123# xyz 456": "(\w+) (\d+)", ["f1", "f2"], True  ==> f1: abc f2: 123

    # full match: ^$
    # ignore group: (?:...)
    # case insensitive: (?i)
    """

    def __init__(self, pattern, fields_info=None):
        self.config = pattern
        try:
            self.ptn = re.compile(pattern)
        except re.error as ex:
            logger.error('transform_regex: cannot compile pattern: "{0}"'.format(ex))
            raise SettingError(ex, pattern)

        self.fields_info = None
        if isinstance(fields_info, (six.text_type, six.binary_type, list, dict)):
            self.fields_info = fields_info
        elif fields_info is not None:
            logger.warning('transform_regex: unknown fields info type: "{0}"'.format(fields_info))

    def __call__(self, event, inpt):
        if not isinstance(inpt, list):
            inputs = [inpt]
        else:
            inputs = inpt

        for data in inputs:
            if data in event:
                m = None
                find_iter = iter(self.ptn.finditer(event[data]))
                for x in find_iter:
                    m = x  # only find the first one cause there's no field info, no meaning to find others.
                    break

                if m:
                    event.update(m.groupdict())
                else:
                    logger.info('transform_regex: field value "{0}" cannot extract value with config "{1}"'
                                .format(event[data], self.config))

                if self.fields_info:
                    # use m first
                    if not m:
                        logger.warning(
                            'transform_regex: field value "{0}" cannot extract value '
                            'with config "{1}" with field info {2}'.format(event[data], self.config, self.fields_info))
                        break

                    if isinstance(self.fields_info, (six.binary_type, six.text_type)):
                        # only find first one
                        event[self.fields_info] = m.group()
                    elif isinstance(self.fields_info, (dict, )):
                        ms = [m] + list(find_iter)

                        for i, m in enumerate(ms):
                            for k, v in six.iteritems(self.fields_info):
                                try:
                                    event[m.expand(k)] = m.expand(v)
                                except re.error as ex:
                                    logger.info(
                                        'transform_regex: cannot expand matched group "{0}" for fields info "{1}:{2}", '
                                        'detail: "{3}"'.format(
                                            m.group(), k, v, ex
                                        ))
                    elif isinstance(self.fields_info, list):
                        if m.groups():  # there's group, use group mode
                            for i, g in enumerate(m.groups()):
                                if i >= len(self.fields_info):
                                    logger.warning(
                                        'transform_regex: field value "{0}" captured group count not equal to '
                                        'config "{1}" with field info {2}'.format(event[data],
                                                                                  self.config, self.fields_info))
                                    break
                                event[self.fields_info[i]] = g
                        else:
                            ms = [m] + list(find_iter)
                            if len(ms) != len(self.fields_info):
                                logger.warning(
                                    'transform_regex: field value "{0}" match count not equal to '
                                    'config "{1}" with field info {2}'.format(event[data], self.config,
                                                                              self.fields_info))
                            for i, m in enumerate(ms):
                                if i >= len(self.fields_info):
                                    break
                                event[self.fields_info[i]] = m.group()
                    else:
                        logger.warning('transform_regex: unknown fields info type: "{0}"'.format(self.fields_info))
            else:
                logger.info('transform_regex: event "{0}" doesn not contain field "{1}"'.format(event, data))

        return event
