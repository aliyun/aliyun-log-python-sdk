class trans_comp_base(object):
    pass


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
