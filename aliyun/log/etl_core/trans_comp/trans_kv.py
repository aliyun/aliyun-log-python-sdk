import logging
from collections import Iterable

import re
import six

from .trans_base import trans_comp_base
from ..etl_util import cached, get_re_full_match

__all__ = ['trans_comp_kv']

logger = logging.getLogger(__name__)


def trans_comp_kv(*args, **kwargs):
    if (args and isinstance(args[0], dict)) or ('event' in kwargs and isinstance(kwargs['event'], dict)):
        # event case
        kv = kv_transformer()
        return kv(*args, **kwargs)
    else:
        return kv_transformer(*args, **kwargs)


def _get_check_fn(skip_if_src_exist, skip_if_src_not_empty, skip_if_value_empty):
    def if_skip(event, key, value):
        if skip_if_src_exist and key in event:
            return True
        if skip_if_src_not_empty and key in event and event[key]:
            return True
        if skip_if_value_empty and not value:
            return True

        return False
    return if_skip


class kv_transformer(trans_comp_base):
    DEFAULT_SEP = u'='
    DEFAULT_QUOTE = u'"'
    DEFAULT_KEYWORD_PTN = u'[\u4e00-\u9fa5\u0800-\u4e00a-zA-Z][\u4e00-\u9fa5\u0800-\u4e00\\w\\.\\-]*'
    SET_MODE = {
                "fill": _get_check_fn(False, True, False),
                "add": _get_check_fn(True, False, False),
                "overwrite": _get_check_fn(False, False, False),
                "fill-auto": _get_check_fn(False, True, True),
                "add-auto": _get_check_fn(True, False, True),
                "overwrite-auto": _get_check_fn(False, False, True)
                }
    DEFAULT_SET_MODE = 'fill-auto'

    @staticmethod
    @cached
    def _get_kv_ptn(sep, quote):
        p1 = u'(?!{0})([\u4e00-\u9fa5\u0800-\u4e00\\w\\.\\-]+)\\s*{0}\\s*([\u4e00-\u9fa5\u0800-\u4e00\\w\\.\\-]+)'
        p2 = u'(?!{0})([\u4e00-\u9fa5\u0800-\u4e00\\w\\.\\-]+)\\s*{0}\\s*{1}\s*([^{1}]*?)\s*{1}'
        ps = u'|'.join([p1, p2]).format(sep, quote)

        logger.info(u"trans_comp_kv: get ptn: {0}".format(ps))
        return re.compile(ps)

    def __init__(self, prefix=None, suffix=None, sep=None, quote=None, mode=None):
        self.prefix = self._u("" if prefix is None else prefix)
        self.suffix = self._u("" if suffix is None else suffix)

        sep = self._u(self.DEFAULT_SEP if sep is None else sep)
        quote = self._u(self.DEFAULT_QUOTE if quote is None else quote)
        self.ptn = self._get_kv_ptn(sep, quote)
        self.kw_ptn = get_re_full_match(self.DEFAULT_KEYWORD_PTN)
        self.skip_if = self.SET_MODE.get(mode, self.SET_MODE[self.DEFAULT_SET_MODE])

    def _extract_kv(self, event, value):
        if isinstance(value, six.binary_type):
            value = value.decode('utf8', 'ignore')

        r = self.ptn.findall(value)

        new_event = {}
        for k1, v1, k2, v2 in r:
            if k1 and self.kw_ptn(k1) and not self.skip_if(event, k1, v1):
                new_event[u"{0}{1}{2}".format(self.prefix, k1, self.suffix)] = v1
            elif k2 and self.kw_ptn(k2) and not self.skip_if(event, k2, v2):
                new_event[u"{0}{1}{2}".format(self.prefix, k2, self.suffix)] = v2

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
                    logger.info(u'trans_comp_lookup: event "{0}" doesn not contain field "{1}"'.format(event, i))
                    continue

                # get input value
                new_event = self._extract_kv(event, event[i])
                event.update(new_event)
        else:
            logger.error(u"trans_comp_lookup: unknown type of input field {0}".format(inpt))

        return event
