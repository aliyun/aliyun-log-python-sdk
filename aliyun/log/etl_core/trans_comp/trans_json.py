from .trans_base import trans_comp_base
import six
import re
import logging
import json
from ..etl_util import cached
from collections import Iterable

__all__ = ['trans_comp_json']

logger = logging.getLogger(__name__)


def trans_comp_json(*args, **kwargs):
    if (args and isinstance(args[0], dict)) or ('event' in kwargs and isinstance(kwargs['event'], dict)):
        # event case
        trans = json_transformer()
        return trans(*args, **kwargs)
    else:
        return json_transformer(*args, **kwargs)


class json_transformer(trans_comp_base):
    DEFAULT_SEP = '.'

    def __init__(self, expand=None, prefix=None, suffix=None, jmes=None, output=None):
        self.expand = expand
        if expand is None:
            self.expand = not jmes
        # self.level = level or 1
        self.jmes = jmes or ""
        self.prefix = "" if prefix is None else prefix
        self.suffix = "" if suffix is None else suffix
        self.output = output
        # self.sep = self.DEFAULT_SEP if sep is None else sep

    def extract_json(self, message):
        new_event = {}
        if isinstance(message, (six.binary_type, six.text_type)):
            try:
                message = json.loads(message)
            except Exception as ex:
                logger.info("json_transformer: fail to load event into json object: {0}, error: {1}".format(message, ex))
                return message

        if isinstance(message, dict):
            for k, v in six.iteritems(message):
                new_event["{0}{1}{2}".format(self.prefix, self._n(k), self.suffix)] = self._n(v)

        return new_event

    def __call__(self, event, inpt):
        # simple dict mode
        if isinstance(inpt, (six.binary_type, six.text_type)):
            inpt = [inpt]

        if isinstance(inpt, Iterable):
            for i in inpt:
                if not isinstance(i, (six.binary_type, six.text_type)):
                    logger.error('trans_comp_lookup: type of input field "{0}" is unknown'.format(i))
                    continue

                if i not in event:
                    logger.info('trans_comp_lookup: event "{0}" doesn not contain field "{1}"'.format(event, i))
                    continue

                # get input value
                new_event = self.extract_json(event[i])
                if event is not new_event:
                    event.update(new_event)
        else:
            logger.error("trans_comp_lookup: unknown type of input field {0}".format(inpt))

        return event
