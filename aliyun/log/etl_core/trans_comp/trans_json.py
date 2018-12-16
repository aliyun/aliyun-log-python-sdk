from .trans_base import trans_comp_base
import six
import re
import logging
import json
from ..etl_util import cached
from collections import Iterable
import jmespath
from jmespath.exceptions import ParseError
from ..exceptions import SettingError

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

    def __init__(self, expand=None, prefix=None, suffix=None, jmes=None, output=None, jmes_ignore_none=None):
        self.expand = expand
        if expand is None:
            self.expand = not jmes
        # self.level = level or 1
        self.jmes = jmes or ""
        self.prefix = "" if prefix is None else prefix
        self.suffix = "" if suffix is None else suffix
        self.output = output
        self.jmes_filter = None
        self.jmes_ignore_none = True if jmes_ignore_none is None else jmes_ignore_none
        if jmes:
            try:
                self.jmes_filter = jmespath.compile(jmes)
            except jmespath.exceptions.ParseError as ex:
                raise SettingError(ex=ex, msg="Invalid JMES filter setting", settings=jmes)

    def _expand_json(self, message, event, prefix=""):
        if isinstance(message, dict):
            for k, v in six.iteritems(message):
                event["{0}{1}{2}".format(self.prefix, self._n(k), self.suffix)] = self._n(v)

    def _process_message(self, message, filter=None):
        new_event = {}
        if isinstance(message, (six.binary_type, six.text_type)):
            try:
                message = json.loads(message)
            except Exception as ex:
                logger.info("json_transformer: fail to load event into json object: {0}, error: {1}".format(message, ex))
                return None

        if filter:
            try:
                message = filter.search(message)
                if message is None and self.jmes_ignore_none:
                    logger.info("split_event_transformer: value {0} get null from jmes settings {1}, skip it".
                                format(message, self.jmes))
                    return None
            except Exception as ex:
                logger.info("split_event_transformer: value {0} with invalid jmes settings {1}, skip it".
                            format(message, self.jmes))
                return None

            if self.output:
                new_event[self.output] = self._n(message)

        # if need to expand
        if self.expand:
            self._expand_json(message, new_event)

        return new_event

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
                    logger.info('trans_comp_lookup: event "{0}" does not contain field "{1}"'.format(event, i))
                    continue

                # get input value
                new_event = self._process_message(event[i], self.jmes_filter)
                if new_event and isinstance(new_event, dict):
                    event.update(new_event)
                else:
                    logger.info('trans_comp_lookup: event "{0}" does not extract value from field "{1}"'.format(event, i))
        else:
            logger.error("trans_comp_lookup: unknown type of input field {0}".format(inpt))

        return event
