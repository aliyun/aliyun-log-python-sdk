from functools import wraps
import re


def cached(fn):
    @wraps(fn)
    def _wrapped(*args, **kwargs):
        sig = str(args) + str(kwargs)
        if not hasattr(fn, "__CACHE__") or sig not in fn.__CACHE__:
            setattr(fn, "__CACHE__", {})

            ret = fn(*args, **kwargs)
            fn.__CACHE__[sig] = ret
            return ret
        else:
            return fn.__CACHE__[sig]

    return _wrapped


def re_full_match(pattern, string, *args, **kwargs):
    if hasattr(re, 'fullmatch'):
        return re.fullmatch(pattern, string, *args, **kwargs)
    m = re.match(pattern, string, *args, **kwargs)
    if m and m.span()[1] == len(string):
        return m


def get_re_full_match(pattern, flags=0):
    p = re.compile(pattern, flags=flags)
    if hasattr(p, 'fullmatch'):
        return p.fullmatch

    def ptn_full_match(string, *args, **kwargs):
        m = p.match(string, *args, **kwargs)
        if m and m.span()[1] == len(string):
            return m

    return ptn_full_match