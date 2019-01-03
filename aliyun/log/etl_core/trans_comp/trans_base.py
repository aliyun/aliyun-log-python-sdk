import json
import six
from ..etl_util import u

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
