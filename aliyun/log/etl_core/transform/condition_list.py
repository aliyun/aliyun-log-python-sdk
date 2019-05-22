"""
CONDITION LIST

list of condition (or),
          if it's dict, each item, it's (and),
               for dict's value, if it's string, should be regex,
                                 if it's lambda, should be predict with value of the field passed,
                                        - Note: when None is passed, it means not existing....
                                 if it's boolean, should be the value
          if it's lambda, should be predict with event passed
          if it's boolean, should be the value
if the value is not list, v => [v]

"""

import logging
try:
    from collections.abc import Callable
except ImportError:
    from collections import Callable

import functools
import six

from ..etl_util import re_full_match, get_re_full_match, u, NOT
from ..exceptions import SettingError

logger = logging.getLogger(__name__)

__all__ = ['condition']


def _get_check(c):
    if isinstance(c, bool):
        return lambda e: c
    elif isinstance(c, Callable):
        return c
    elif isinstance(c, (dict,)):
        def check(event):
            for k, v in six.iteritems(c):
                if k in event:
                    if (isinstance(v, bool) and v) \
                            or (isinstance(v, Callable) and v(event[k])) \
                            or (isinstance(v, (six.text_type, six.binary_type))
                                and re_full_match(v, event[k])):
                        continue

                    return False
                else:
                    if v is None:
                        continue

                    return False

            return True

        return check


class condition(object):
    def __init__(self, cond, pass_meta=None, restore_meta=None):
        """

        :param cond:
        :param pass_meta: if pass meta fields like __time__, __topic__, __tag:xxx__ to processor, by default True
        :param restore_meta: restore meta after processor. by default (None), False for pass_meta=True and True for pass_meta=False,
        """
        cond = u(cond)

        if not isinstance(cond, list):
            self.cond = [cond]
        else:
            self.cond = cond

        self.pass_meta = pass_meta
        self.restore_meta = restore_meta

        if self.pass_meta is None:
            self.pass_meta = True

        if self.restore_meta is None:
            self.restore_meta = not self.pass_meta

        self.check_list = []
        for c in self.cond:
            ck = _get_check(c)
            if ck is not None:
                self.check_list.append(ck)
            else:
                raise SettingError(msg="Invalid condition", settings=c)

    DEFAULT_META_KEYS = set(("__time__", "__topic__", "__source__"))
    tag_meta_check = staticmethod(get_re_full_match(r"__tag__:.+"))

    def is_meta_key(self, k):
        return k in self.DEFAULT_META_KEYS or self.tag_meta_check(k)

    def call_processor(self, fn, evt, *args, **kwargs):
        event = evt
        args = u(args)
        kwargs = u(kwargs)

        if not self.pass_meta:
            # no meta version
            event = dict((k, v) for k, v in six.iteritems(event) if not self.is_meta_key(k))

        if not self.restore_meta:
            return fn(event, *args, **kwargs)
        else:
            # restore meta
            meta = dict((k, v) for k, v in six.iteritems(evt) if self.is_meta_key(k))
            ret = fn(event, *args, **kwargs)
            if ret is not None:
                ret.update(meta)
            return ret

    def __call__(self, entity):
        entity = u(entity)

        if isinstance(entity, (dict,)):
            return any(c(entity) for c in self.check_list)
        elif isinstance(entity, Callable):
            fn = entity

            @functools.wraps(fn)
            def _wrapped(event, *args, **kwargs):
                try:
                    if any(c(event) for c in self.check_list):
                        return self.call_processor(fn, event, *args, **kwargs)
                except Exception as ex:
                    logger.error(
                        u'fail to call hooked function "{0}" with event "{1}", error: {2}'.format(fn, event, ex))

                return event

            return _wrapped
        else:
            errors = u"condition: unsupported data type: {0}".format(entity)
            logger.error(errors)
            raise SettingError(settings=errors)
