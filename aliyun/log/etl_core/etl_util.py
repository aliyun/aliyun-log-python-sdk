from functools import wraps
import re
import copy


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


@cached
def get_re_full_match(pattern, flags=0):
    p = re.compile(pattern, flags=flags)
    if hasattr(p, 'fullmatch'):
        return p.fullmatch

    def ptn_full_match(string, *args, **kwargs):
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
