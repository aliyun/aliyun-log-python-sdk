from .transform_list import transform
from .condition_list import condition
from functools import wraps
from .transform_base import transform_base
from ..etl_util import process_event, support_event_list_simple
import logging

__all__ = ['dispatch_event', 'transform_event', 'drop_event', 'keep_event']

logger = logging.getLogger(__name__)


class dispatch_event(transform_base):
    def __init__(self, config):
        if not isinstance(config, list):
            self.trans = [config]
        else:
            self.trans = config

        self.trans = [(condition(c), transform(t)) for c, t in self.trans]

    @support_event_list_simple
    def __call__(self, event):
        for c, t in self.trans:
            if c(event):  # only match once
                return t(event)

        return event


# this function is used to bypass lambda trap
def _get_ct(c, t):
    @wraps(t)
    def _fn(e):
        if c(e):
            return t(e)
        return e
    return _fn


class transform_event(transform_base):
    def __init__(self, config):
        if not isinstance(config, list):
            trans = [config]
        else:
            trans = config

        self.fn_list = [_get_ct(condition(c), transform(t)) for c, t in trans]

    def __call__(self, event):
        return process_event(event, self.fn_list)


class drop_event(transform_base):
    def __init__(self, cond):
        self.c = condition(cond)

    @support_event_list_simple
    def __call__(self, event):
        if self.c(event):
            logger.info(u"drop_event: drop event: {0}".format(event))
            return None
        return event


class keep_event(transform_base):
    def __init__(self, cond):
        self.c = condition(cond)

    @support_event_list_simple
    def __call__(self, event):
        if self.c(event):
            return event
        logger.info(u"keep_event: drop event: {0}".format(event))
        return None
