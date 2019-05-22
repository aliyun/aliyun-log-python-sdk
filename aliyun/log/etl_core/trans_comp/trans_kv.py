import logging
try:
    from collections.abc import Iterable
except ImportError:
    from collections import Iterable

import re
import six

from .trans_base import trans_comp_check_mdoe_base
from ..etl_util import cached, get_re_full_match, get_set_mode_if_skip_fn

__all__ = ['trans_comp_kv']

logger = logging.getLogger(__name__)


def trans_comp_kv(*args, **kwargs):
    if (args and isinstance(args[0], dict)) or ('event' in kwargs and isinstance(kwargs['event'], dict)):
        # event case
        kv = kv_transformer()
        return kv(*args, **kwargs)
    else:
        return kv_transformer(*args, **kwargs)


class kv_transformer(trans_comp_check_mdoe_base):
    DEFAULT_SEP = u'='
    DEFAULT_QUOTE = u'"'

    @staticmethod
    @cached
    def _get_kv_ptn(sep, quote, escape):
        p1 = u'(?!{0})([\u4e00-\u9fa5\u0800-\u4e00\\w\\.\\-]+)\\s*{0}\\s*([\u4e00-\u9fa5\u0800-\u4e00\\w\\.\\-]+)'
        if not escape:
            p2 = u'(?!{0})([\u4e00-\u9fa5\u0800-\u4e00\\w\\.\\-]+)\\s*{0}\\s*{1}\s*([^{1}]*?)\s*{1}'
        else:
            p2 = u'(?!{0})([\u4e00-\u9fa5\u0800-\u4e00\\w\\.\\-]+)\\s*{0}\\s*{1}\s*((?:[^{1}]|\\\\{1})*?[^\\\\]){1}'
        ps = u'|'.join([p1, p2]).format(sep, quote)

        logger.info(u"trans_comp_kv: get ptn: {0}".format(ps))
        return re.compile(ps)

    def __init__(self, prefix=None, suffix=None, sep=None, quote=None, escape=None, mode=None):
        super(kv_transformer, self).__init__(mode=mode)
        self.prefix = self._u("" if prefix is None else prefix)
        self.suffix = self._u("" if suffix is None else suffix)
        self.escape = False if escape is None else escape

        self.sep = self._u(self.DEFAULT_SEP if sep is None else sep)
        self.quote = self._u(self.DEFAULT_QUOTE if quote is None else quote)
        self.ptn = self._get_kv_ptn(self.sep, self.quote, self.escape)
        self.normalize = None

    def set(self, e, k, v, real_k=None, check_kw_name=False):
        """override base to handle escape case: replace \" to " """
        if self.escape:
            v = v.strip().replace("\\" + self.quote, self.quote)
        return super(kv_transformer, self).set(e, k, v, real_k=real_k, check_kw_name=check_kw_name)

    def _extract_kv(self, event, value):
        if isinstance(value, six.binary_type):
            value = value.decode('utf8', 'ignore')

        r = self.ptn.findall(value)

        new_event = {}
        for k1, v1, k2, v2 in r:
            if k1:
                self.set(event, k1, v1, real_k=u"{0}{1}{2}".format(self.prefix, k1, self.suffix), check_kw_name=True)
            if k2:
                self.set(event, k2, v2, real_k=u"{0}{1}{2}".format(self.prefix, k2, self.suffix), check_kw_name=True)

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
