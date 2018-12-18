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
