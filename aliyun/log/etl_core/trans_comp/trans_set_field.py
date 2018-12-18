from .trans_base import trans_comp_base
import json
import logging
import six
import csv

logger = logging.getLogger(__name__)

__all__ = ['V', 'trans_set_field_zip']


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


class trans_set_field_zip(trans_comp_base):
    DEFAULT_COMBINE_SEP = '#'
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
        self.field1 = field1
        self.field2 = field2
        self.combine_sep = combine_sep or self.DEFAULT_COMBINE_SEP
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
            logger.debug("split_event_transformer: value {0} is not json, try to use csv".format(v))

        # continue to parse it as csv
        if isinstance(v, (six.text_type, six.binary_type)):
            result = list(csv.reader([v], skipinitialspace=lstrip, delimiter=sep, quotechar=quote))[0]
            return result
        else:
            logger.info("split_event_transformer: cannot extract list from value: {0}".format(v))
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
            data = ["{0}{1}{2}".format(self._n(v[0]), self.combine_sep, self._n(v[1])) for v in zip(ldata, rdata)]
            writer.writerow(data)

            return buf.getvalue().strip()
        else:
            logger.error("trans_set_field_zip: event '{0}' doesn't contain both fields: {1}, {2}"
                         .format(event, self.field1, self.field2))

        return event
