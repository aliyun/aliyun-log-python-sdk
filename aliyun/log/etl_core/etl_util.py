from functools import wraps
import re
import copy
import six

DEFAULT_FN_CACHE_SIZE = 102400  # 100K

def cached(fn):
    @wraps(fn)
    def _wrapped(*args, **kwargs):
        sig = str(args) + str(kwargs)
        if not hasattr(fn, "__CACHE__") or len(fn.__CACHE__) > DEFAULT_FN_CACHE_SIZE:
            setattr(fn, "__CACHE__", {})

        if sig not in fn.__CACHE__:
            ret = fn(*args, **kwargs)
            fn.__CACHE__[sig] = ret
            return ret
        else:
            return fn.__CACHE__[sig]

    return _wrapped


def re_full_match(pattern, string, *args, **kwargs):
    is_not = isinstance(pattern, NOT)
    if six.PY2 and isinstance(pattern, six.binary_type):
        pattern = pattern.decode('utf8', 'ignore')
    if six.PY2 and isinstance(string, six.binary_type):
        string = string.decode('utf8', 'ignore')

    ret = None
    if hasattr(re, 'fullmatch'):
        ret = re.fullmatch(pattern, string, *args, **kwargs)
    else:
        m = re.match(pattern, string, *args, **kwargs)
        if m and m.span()[1] == len(string):
            ret = m

    return not ret if is_not else ret


# this function is used to bypass lambda trap
def bind_event_fn(fn, *args, **kwargs):
    @wraps(fn)
    def _real_fn(e):
        return fn(e, *args, **kwargs)

    return _real_fn


@cached
def get_re_full_match(pattern, flags=0):
    if six.PY2 and isinstance(pattern, six.binary_type):
        pattern = pattern.decode('utf8', 'ignore')

    p = re.compile(pattern, flags=flags)
    if hasattr(p, 'fullmatch'):  # normally it's Py3
        return p.fullmatch

    def ptn_full_match(string, *args, **kwargs):
        if six.PY2 and isinstance(string, six.binary_type):
            string = string.decode('utf8', 'ignore')

        m = p.match(string, *args, **kwargs)
        if m and m.span()[1] == len(string):
            return m

    return ptn_full_match


def process_event(event, fn_list, cp=True):
    if not len(fn_list):
        return event

    new_event = event
    if cp:
        new_event = copy.copy(event)

    if isinstance(new_event, dict):
        new_event = fn_list[0](new_event)
        return process_event(new_event, fn_list[1:], cp=False)
    elif isinstance(new_event, (tuple, list)):
        result = []
        for e in new_event:
            new_e = fn_list[0](e)
            ret = process_event(new_e, fn_list[1:], cp=False)
            if ret is None:
                continue

            if isinstance(ret, (tuple, list)):
                result.extend(ret)
            else:
                result.append(ret)

        if result:
            if len(result) == 1:
                return result[0]
            return result
        return None  # return None for empty list

    return new_event


def _is_event_list(event):
    if isinstance(event, (list, tuple)):
        for e in event:
            if e is not None and not isinstance(e, dict):
                break
        else:
            return True

    return False


def support_event_list_simple(fn):
    """
    enable __call__ to accept event_list.
    :param fn:
    :return:
    """
    @wraps(fn)
    def _wrapped(self, event, *args, **kwargs):
        if _is_event_list(event):
            result = []
            for e in event:
                ret = fn(self, e, *args, **kwargs)

                if ret is None:
                    continue

                if isinstance(ret, (tuple, list)):
                    result.extend(ret)
                else:
                    result.append(ret)

            if result:
                if len(result) == 1:
                    return result[0]
                return result
            return None
        else:
            return fn(self, event, *args, **kwargs)

    return _wrapped


def u(d):
    """
    convert string, string container or unicode
    :param d:
    :return:
    """
    if six.PY2:
        if isinstance(d, six.binary_type):
            return d.decode("utf8", "ignore")
        elif isinstance(d, list):
            return [u(x) for x in d]
        elif isinstance(d, tuple):
            return tuple(u(x) for x in d)
        elif isinstance(d, dict):
            return dict( (u(k), u(v)) for k, v in six.iteritems(d))

    return d


class NOT(object):
    def __new__(self, v):
        if isinstance(v, six.binary_type):
            return _NOT_B(v)
        elif isinstance(v, six.text_type):
            return _NOT_U(v)
        else:
            raise ValueError("NOT can only be used with string. ")


class _NOT_B(six.binary_type, NOT):
    def __init__(self, v):
        super(_NOT_B, self).__init__(v)
        self.v = v

    def decode(self, *args, **kwargs):
        return _NOT_B(self.v.decode(*args, **kwargs))


class _NOT_U(six.text_type, NOT):
    def __init__(self, v):
        if six.PY2:
            super(_NOT_U, self).__init__(v)
        self.v = v

    def encode(self, *args, **kwargs):
        return _NOT_B(self.v.encode(*args, **kwargs))


def get_set_mode_if_skip_fn(skip_if_src_exist, skip_if_src_not_empty, skip_if_value_empty):
    def if_skip(event, key, value):
        if skip_if_src_exist and key in event:
            return True
        if skip_if_src_not_empty and key in event and event[key]:
            return True
        if skip_if_value_empty and not value:
            return True

        return False
    return if_skip
