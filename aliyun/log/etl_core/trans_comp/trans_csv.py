from .trans_base import trans_comp_base
import csv
import six
import re
from collections import Iterable
from ..exceptions import SettingError
import logging

__all__ = ['trans_comp_csv', 'trans_comp_tsv']

logger = logging.getLogger(__name__)


class trans_comp_csv(trans_comp_base):
    p_csv_sep = re.compile(r'\s*,\s*')
    DEFAULT_SEP = ','
    DEFAULT_QUOTE = '"'

    def __init__(self, config, sep=None, quote=None, lstrip=None, restrict=None):
        if isinstance(config, (six.text_type, six.binary_type) ):
            self.keys = self.p_csv_sep.split(config)
        elif isinstance(config, Iterable):
            self.keys = list(config)
        else:
            raise SettingError(settings=config)

        self.sep = sep or self.DEFAULT_SEP
        self.lstrip = True if lstrip is None else lstrip
        self.quote = quote or self.DEFAULT_QUOTE
        self.restrict = False if restrict is None else restrict

    def __call__(self, event, inpt):
        if inpt in event:
            data = event[inpt]
            ret = list(csv.reader([data], skipinitialspace=self.lstrip, delimiter=self.sep, quotechar=self.quote))[0]
            if self.restrict and len(ret) != len(self.keys):
                logger.warn("event {0} field {1} contains different count of fields as expected key {2} actual {3}".format(event, inpt, self.keys, ret))
                return event

            new_event = dict(zip(self.keys, ret))
            event.update(new_event)
        else:
            logger.warn("field {0} doesn't exist in event {1}, skip it".format(inpt, event))

        return event


class trans_comp_tsv(trans_comp_csv):
    DEFAULT_SEP = '\t'

