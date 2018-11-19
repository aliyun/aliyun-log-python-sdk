from .trans_base import trans_comp_base

__all__ = ['trans_comp_csv', 'trans_comp_tsv', 'trans_comp_lookup', 'trans_comp_json', 'trans_comp_kv']


class trans_comp_csv(trans_comp_base):
    def __init__(self, config):
        self.config = config

    def __call__(self, event, inpt):
        return event


class trans_comp_tsv(trans_comp_base):
    def __init__(self, config):
        self.config = config

    def __call__(self, event, inpt):
        return event


class trans_comp_lookup(trans_comp_base):
    def __init__(self, config):
        self.config = config

    def __call__(self, event, inpt):
        return event


class trans_comp_json(trans_comp_base):
    def __init__(self, config):
        self.config = config

    def __call__(self, event, inpt):
        return event


class trans_comp_kv(trans_comp_base):
    def __init__(self, config):
        self.config = config

    def __call__(self, event, inpt):
        return event
