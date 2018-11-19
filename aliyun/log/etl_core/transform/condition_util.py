
__all__ = ['F_TAGS', 'F_META', 'F_TIME',
           'EMPTY', 'EXIST', 'NONE', 'NO_EMPTY', 'ANY', 'ALL']

ALL, ANY = True, True

F_TAGS = '__tag__:.+'
F_META = '__tag__:.+|__topic__|__source__'
F_TIME = '__time__'


class EMPTY(object):
    def __init__(self, field):
        self.field = field

    def __call__(self, event):
        return self.field not in event or not event[self.field]


class NONE(object):
    def __init__(self, field):
        self.field = field

    def __call__(self, event):
        return self.field not in event


class EXIST(object):
    def __init__(self, field):
        self.field = field

    def __call__(self, event):
        return self.field in event


class NO_EMPTY(object):
    def __init__(self, field):
        self.field = field

    def __call__(self, event):
        return self.field in event and event[self.field]
