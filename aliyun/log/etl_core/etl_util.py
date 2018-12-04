from functools import wraps


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
