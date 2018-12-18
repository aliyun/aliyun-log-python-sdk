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


# this function is used to bypass lambda trap
def bind_event_fn(fn, *args, **kwargs):
    @wraps(fn)
    def _real_fn(e):
        return fn(e, *args, **kwargs)

    return _real_fn


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