from .transform_list import transform
from .condition_list import condition

from .transform_base import transform_base
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

    def __call__(self, event):
        for c, t in self.trans:
            if c(event):  # only match once
                return t(event)

        return event


class transform_event(transform_base):
    def __init__(self, config):
        if not isinstance(config, list):
            self.trans = [config]
        else:
            self.trans = config

        self.trans = [(condition(c), transform(t)) for c, t in self.trans]

    def __call__(self, event):
        for c, t in self.trans:
            if c(event):
                event = t(event)

        return event


class drop_event(transform_base):
    def __init__(self, cond):
        self.c = condition(cond)

    def __call__(self, event):
        if self.c(event):
            logger.info("drop_event: drop event: {0}".format(event))
            return None
        return event


class keep_event(transform_base):
    def __init__(self, cond):
        self.c = condition(cond)

    def __call__(self, event):
        if self.c(event):
            return event
        logger.info("keep_event: drop event: {0}".format(event))
        return None


