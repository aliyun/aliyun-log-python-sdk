from .trans_base import trans_comp_base
import csv
import six
import logging
import copy
import json
import jmespath
from jmespath.exceptions import ParseError
from ..exceptions import SettingError

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

    def __init__(self, sep=None, quote=None, lstrip=None, output=None, jmes=None):
        self.sep = sep or self.DEFAULT_SEP
        self.lstrip = True if lstrip is None else lstrip
        self.quote = quote or self.DEFAULT_QUOTE
        self.output_field = self._u(output)

        self.jmes = self._u(jmes or "")
        self.jmes_filter = None
        if jmes:
            try:
                self.jmes_filter = jmespath.compile(jmes)
            except jmespath.exceptions.ParseError as ex:
                raise SettingError(ex=ex, msg="Invalid JMES filter setting", settings=jmes)

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
            event[field] = self._n(lst[0])
            return event

    def _parse_list(self, v, flt=None):
        # process json first (via filter or directly list)
        try:
            ret = json.loads(v)

            # list and not jmes filter
            if isinstance(ret, (list,)) and not self.jmes:
                return ret

            # json object and jmes is configured
            if flt:
                ret = flt.search(ret)

                if isinstance(ret, (list,)):
                    # result is list as expected
                    return ret
                elif isinstance(ret, (six.text_type, six.binary_type)):
                    # result is just a string. then re-do the process and no jmes is passed this time
                    return self._parse_list(ret)
                else:
                    logger.info(
                        u'split_event_transformer: get unknown type of result with value "{0}" and jmes filter "{1}", skip it'.
                        format(v, self.jmes))
                    return None

            # else: go to next step: parse it as CSV

        except Exception as ex:
            # failed at json load or jmes filter. if jmes is configured, then it's an invalid event
            if flt:
                logger.info(u"split_event_transformer: value {0} is json or not invaid jmes settings {1}, skip it".
                            format(v, self.jmes))
                return None
            else:
                logger.debug(u"split_event_transformer: value {0} is not json, try to use csv".format(v))

        # continue to parse it as csv
        if isinstance(v, (six.text_type, six.binary_type)):
            result = list(csv.reader([v], skipinitialspace=self.lstrip, delimiter=self.sep, quotechar=self.quote))[0]
            return result
        else:
            logger.info(u"split_event_transformer: cannot extract list from value: {0}".format(v))
            return None

    def __call__(self, event, inpt):
        inpt = self._u(inpt)

        # overwrite input field
        if not self.output_field:
            self.output_field = inpt

        if inpt in event:
            # json/csv mode
            ret = self._parse_list(event[inpt], self.jmes_filter)
            return self._process_list(event, self.output_field, ret)
        else:
            logger.error(u"trans_comp_lookup: unknown type of input field {0}".format(inpt))

        return event
