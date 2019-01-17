from .trans_base import trans_comp_base
import json
import logging
import six
import csv
from ..exceptions import SettingError

logger = logging.getLogger(__name__)

__all__ = ['V', 'trans_set_field_zip']


class V(trans_comp_base):
    def __init__(self, *config):
        if not config:
            raise SettingError(settings=config)

        config = self._u(config)
        self.field = config

    def _get_value(self, e):
        # it's string list
        for field in self.field:
            if field in e:
                return e.get(field, None)

    def __call__(self, event, inpt=None):
        inpt = self._u(inpt)

        if inpt is None:
            # it's a field setting value mode (just return value)
            return self._get_value(event)
        else:
            # it's transform mote (do configuration)
            value = self._get_value(event)
            if value is not None:
                event[inpt] = value
            elif inpt in event:
                del event[inpt]

        return event


class trans_set_field_zip(trans_comp_base):
    DEFAULT_COMBINE_SEP = u'#'
    DEFAULT_SEP = ','
    DEFAULT_QUOTE = '"'
    DEFAULT_PARSE = (',', '"')

    def __init__(self, field1, field2, combine_sep=None, sep=None, quote=None,
                 lparse=None, rparse=None):
        """
        :param combine_sep: used to combine two inputs
        :param sep:    used in new output
        :param quote:  used in new output
        :param lparse: (",", '|')
        :param rparse: (",", "|")
        """
        self.field1 = self._u(field1)
        self.field2 = self._u(field2)
        self.combine_sep = self._u(combine_sep or self.DEFAULT_COMBINE_SEP)
        self.sep = sep or self.DEFAULT_SEP
        self.quote = quote or self.DEFAULT_QUOTE
        self.lsep, self.lquote = lparse or self.DEFAULT_PARSE
        self.rsep, self.rquote = rparse or self.DEFAULT_PARSE

    def _parse_list(self, v, sep, quote, lstrip=True):
        try:
            ret = json.loads(v)

            # list and not jmes filter
            if isinstance(ret, (list,)):
                return ret

        except Exception as ex:
            logger.debug(u"split_event_transformer: value {0} is not json, try to use csv".format(v))

        # continue to parse it as csv
        if isinstance(v, (six.text_type, six.binary_type)):
            result = list(csv.reader([v], skipinitialspace=lstrip, delimiter=sep, quotechar=quote))[0]
            return result
        else:
            logger.info(u"split_event_transformer: cannot extract list from value: {0}".format(v))
            return None

    def __call__(self, event):
        """
            spamwriter = csv.writer(csvfile, delimiter=' ',
                                    quotechar='|', quoting=csv.QUOTE_MINIMAL)
            spamwriter.writerow(['Spam'] * 5 + ['Baked Beans'])
            spamwriter.writerow(['Spam', 'Lovely Spam', 'Wonderful Spam'])
        """
        if self.field1 in event and self.field2 in event:
            ldata = self._parse_list(event[self.field1], self.lsep, self.lquote)
            rdata = self._parse_list(event[self.field2], self.rsep, self.rquote)

            buf = six.StringIO()
            writer = csv.writer(buf, delimiter=self.sep, quotechar=self.quote, quoting=csv.QUOTE_MINIMAL)
            data = [u"{0}{1}{2}".format(self._n(v[0]), self.combine_sep, self._n(v[1])) for v in zip(ldata, rdata)]
            writer.writerow(data)

            return buf.getvalue().strip()
        else:
            logger.error(u"trans_set_field_zip: event '{0}' doesn't contain both fields: {1}, {2}"
                         .format(event, self.field1, self.field2))

        return event
