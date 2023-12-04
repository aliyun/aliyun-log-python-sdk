import json
import six
from ..etl_util import u
from ..etl_util import get_re_full_match, get_set_mode_if_skip_fn
import logging

logger = logging.getLogger(__name__)


class trans_comp_base(object):
    @property
    def __name__(self):
        return str(type(self))

    @staticmethod
    def _n(v):
        """
        convert string to utf8 in Py2 or unicode in Py3
        :param v:
        :return:
        """
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

    @staticmethod
    def _u(d):
        """
        convert string, string container or unicode
        :param d:
        :return:
        """
        return u(d)


class trans_comp_check_mdoe_base(trans_comp_base):
    DEFAULT_KEYWORD_PTN = u'[\u4e00-\u9fa5\u0800-\u4e00a-zA-Z][\u4e00-\u9fa5\u0800-\u4e00\\w\\.\\-]*'
    SET_MODE = {
                "fill": get_set_mode_if_skip_fn(False, True, False),
                "add": get_set_mode_if_skip_fn(True, False, False),
                "overwrite": get_set_mode_if_skip_fn(False, False, False),
                "fill-auto": get_set_mode_if_skip_fn(False, True, True),
                "add-auto": get_set_mode_if_skip_fn(True, False, True),
                "overwrite-auto": get_set_mode_if_skip_fn(False, False, True)
                }
    DEFAULT_SET_MODE = 'fill-auto'

    def __init__(self, mode=None):
        super(trans_comp_check_mdoe_base, self).__init__()
        self.kw_ptn = get_re_full_match(self.DEFAULT_KEYWORD_PTN)
        self.skip_if = self.SET_MODE.get(mode, self.SET_MODE[self.DEFAULT_SET_MODE])

    def set(self, e, k, v, real_k=None, check_kw_name=False):
        if not check_kw_name or (check_kw_name and self.kw_ptn(k)):
            real_k = real_k or k
            if k and not self.skip_if(e, k, v):
                e[real_k] = v
                return True

        logger.debug("{1}: skip detected k-v due to current mode: {0}".format((k, v), type(self)))
        return False

    def sets(self, e, e_new, check_kw_name=False):
        has_update = False
        for k, v in six.iteritems(e_new):
            has_update = self.set(e, k, v, check_kw_name=check_kw_name) or has_update

        return has_update
