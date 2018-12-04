from .trans_base import trans_comp_base
import six
import re
import logging
import json
from ..etl_util import cached
from collections import Iterable

__all__ = ['trans_comp_kv']

logger = logging.getLogger(__name__)


def trans_comp_kv(*args, **kwargs):
    if (args and isinstance(args[0], dict)) or ('event' in kwargs and isinstance(kwargs['event'], dict)):
        # event case
        kv = kv_transformer()
        return kv(*args, **kwargs)
    else:
        return kv_transformer(*args, **kwargs)


class kv_transformer(trans_comp_base):
    DEFAULT_SEP = '='
    DEFAULT_QUOTE = '"'

    @staticmethod
    @cached
    def _get_kv_ptn(sep, quote):
        p1 = u'(?!{0})([\u4e00-\u9fa5\u0800-\u4e00\\w\\.\\-]+)\\s*{0}\\s*([\u4e00-\u9fa5\u0800-\u4e00\\w\\.\\-]+)'
        p2 = u'(?!{0})([\u4e00-\u9fa5\u0800-\u4e00\\w\\.\\-]+)\\s*{0}\\s*{1}\s*([^{1}]+?)\s*{1}'
        ps = u'|'.join([p1, p2]).format(sep, quote)

        logger.info(u"trans_comp_kv: get ptn: {0}".format(ps))
        return re.compile(ps)

    @staticmethod
    def _n(v):
        if v is None:
            return ""

        if isinstance(v, (dict, list)):
            try:
                v = json.dumps(v)
            except Exception:
                pass
        elif six.PY2 and isinstance(v, six.text_type):
            v = v.encode('utf8', "ignore")
        elif six.PY3 and isinstance(v, six.binary_type):
            v = v.decode('utf8', "ignore")

        return str(v)

    def __init__(self, prefix=None, suffix=None, sep=None, quote=None):
        self.prefix = "" if prefix is None else prefix
        self.suffix = "" if suffix is None else suffix

        sep = self.DEFAULT_SEP if sep is None else sep
        quote = self.DEFAULT_QUOTE if quote is None else quote
        self.ptn = self._get_kv_ptn(sep, quote)

    def extract_kv(self, value):
        if isinstance(value, six.binary_type):
            value = value.decode('utf8', 'ignore')

        r = self.ptn.findall(value)

        new_event = {}
        for k1, v1, k2, v2 in r:
            if k1:
                new_event["{0}{1}{2}".format(self.prefix, self._n(k1), self.suffix)] = self._n(v1)
            elif k2:
                new_event["{0}{1}{2}".format(self.prefix, self._n(k2), self.suffix)] = self._n(v2)

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
                    logger.info('trans_comp_lookup: event "{0}" doesn not contain field "{1}"'.format(event, i))
                    continue

                # get input value
                new_event = self.extract_kv(event[i])
                event.update(new_event)
        else:
            logger.error("trans_comp_lookup: unknown type of input field {0}".format(inpt))

        return event
