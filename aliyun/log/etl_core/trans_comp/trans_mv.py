from .trans_base import trans_comp_base
import csv
import six
import logging
import copy
import json

__all__ = ['trans_comp_split_event']

logger = logging.getLogger(__name__)


def trans_comp_split_event(*args, **kwargs):
    if (args and isinstance(args[0], dict)) or ('event' in kwargs and isinstance(kwargs['event'], dict)):
        # event case
        split = split_event_transformer()
        return split(*args, **kwargs)
    else:
        return split_event_transformer(*args, **kwargs)


class split_event_transformer(trans_comp_base):
    DEFAULT_SEP = ','
    DEFAULT_QUOTE = '"'

    def __init__(self, sep=None, quote=None, lstrip=None, output_field=None):
        self.sep = sep or self.DEFAULT_SEP
        self.lstrip = True if lstrip is None else lstrip
        self.quote = quote or self.DEFAULT_QUOTE
        self.output_field = output_field

    def _process_list(self, event, field, lst):
        if not lst:
            return event

        if len(lst) > 1:
            result = []
            for d in lst:
                e = copy.copy(event)
                e[field] = self._n(d)
                result.append(e)
            return result
        else:
            event[field] = self._n(lst)
            return event

    def _parse_list(self, v):
        try:
            ret = json.loads(v)
            if isinstance(ret, (list)):
                return ret
        except Exception as ex:
            logger.debug("split_event_transformer: value {0} is not json, try to use csv".format(v))

        result = list(csv.reader([v], skipinitialspace=self.lstrip, delimiter=self.sep, quotechar=self.quote))[0]
        return result

    def __call__(self, event, inpt):
        # overwrite input field
        if not self.output_field:
            self.output_field = inpt

        # csv mode
        if isinstance(inpt, (six.binary_type, six.text_type)):
            ret = self._parse_list(event[inpt])
            return self._process_list(event, inpt, ret)
        else:
            logger.error("trans_comp_lookup: unknown type of input field {0}".format(inpt))

        return event
