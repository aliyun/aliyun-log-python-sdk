import logging
import re
import six

from .trans_base import trans_comp_check_mdoe_base
from ..exceptions import SettingError

logger = logging.getLogger(__name__)

__all__ = ['trans_comp_regex']


class trans_comp_regex(trans_comp_check_mdoe_base):
    """
    # full match: ^$
    # ignore group: (?:...)
    # case insensitive: (?i)
    """

    def __init__(self, pattern, fields_info=None, mode=None):
        super(trans_comp_regex, self).__init__(mode=mode)
        pattern = self._u(pattern)
        fields_info = self._u(fields_info)

        self.config = pattern

        try:
            self.ptn = re.compile(pattern)
        except re.error as ex:
            logger.error(u'transform_regex: cannot compile pattern: "{0}"'.format(ex))
            raise SettingError(ex, pattern)

        self.fields_info = None
        if isinstance(fields_info, (six.text_type, six.binary_type, list, dict)):
            self.fields_info = fields_info
        elif fields_info is not None:
            logger.warning(u'transform_regex: unknown fields info type: "{0}"'.format(fields_info))

    def __call__(self, event, inpt):
        inpt = self._u(inpt)

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
                    self.sets(event, m.groupdict())
                else:
                    logger.info(u'transform_regex: field value "{0}" cannot extract value with config "{1}"'
                                .format(event[data], self.config))

                if self.fields_info:
                    # use m first
                    if not m:
                        logger.warning(
                            u'transform_regex: field value "{0}" cannot extract value '
                            u'with config "{1}" with field info {2}'.format(event[data], self.config, self.fields_info))
                        break

                    if isinstance(self.fields_info, (six.binary_type, six.text_type)):
                        # only find first one
                        k, v = self.fields_info, m.group()
                        self.set(event, k, v)
                    elif isinstance(self.fields_info, (dict, )):
                        ms = [m] + list(find_iter)

                        for i, m in enumerate(ms):
                            for k, v in six.iteritems(self.fields_info):
                                try:
                                    kk, vv = m.expand(k), m.expand(v)
                                    self.set(event, kk, vv, check_kw_name=True)
                                except re.error as ex:
                                    logger.info(
                                        u'transform_regex: cannot expand matched group "{0}" for fields info "{1}:{2}", '
                                        u'detail: "{3}"'.format(
                                            m.group(), k, v, ex
                                        ))
                    elif isinstance(self.fields_info, list):
                        if m.groups():  # there's group, use group mode
                            for i, g in enumerate(m.groups()):
                                if i >= len(self.fields_info):
                                    logger.warning(
                                        u'transform_regex: field value "{0}" captured group count not equal to '
                                        u'config "{1}" with field info {2}'.format(event[data],
                                                                                  self.config, self.fields_info))
                                    break

                                k, v = self.fields_info[i], g
                                self.set(event, k, v, check_kw_name=True)
                        else:
                            ms = [m] + list(find_iter)
                            if len(ms) != len(self.fields_info):
                                logger.warning(
                                    u'transform_regex: field value "{0}" match count not equal to '
                                    u'config "{1}" with field info {2}'.format(event[data], self.config,
                                                                              self.fields_info))
                            for i, m in enumerate(ms):
                                if i >= len(self.fields_info):
                                    break

                                k, v = self.fields_info[i], m.group()
                                self.set(event, k, v, check_kw_name=True)
                    else:
                        logger.warning(u'transform_regex: unknown fields info type: "{0}"'.format(self.fields_info))
            else:
                logger.info(u'transform_regex: event "{0}" doesn not contain field "{1}"'.format(event, data))

        return event
