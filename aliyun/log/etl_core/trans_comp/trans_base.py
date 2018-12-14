import json
import six


class trans_comp_base(object):
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


class V(trans_comp_base):
    def __init__(self, config):
        self.field = config

    def __call__(self, event, inpt=None):
        if inpt is None:
            # it's a field setting value mode (just return value)
            return event.get(self.field, None)
        else:
            # it's transform mote (do configuration)
            if self.field not in event:
                if inpt in event:
                    del event[inpt]
            else:
                event[inpt] = event[self.field]

        return event
