
class transform_base(object):
    def __call__(self, event):
        pass

    @property
    def __name__(self):
        return str(type(self))
