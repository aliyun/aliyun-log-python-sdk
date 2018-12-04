from .trans_base import trans_comp_base
import six
import os
from ..exceptions import SettingError
import csv
from collections import Iterable
import logging
import inspect

logger = logging.getLogger(__name__)

__all__ = ['trans_comp_lookup', 'trans_comp_json']


class LookupError(SettingError):
    pass


class Table(object):
    def __init__(self, reader, case_insensitive):
        self.field_names = set(reader.fieldnames)
        self.rows = [row for row in reader]
        self.case_insensitive = case_insensitive

    def get_row(self, inputs):
        assert isinstance(inputs, dict), LookupError(msg="trans_comp_lookup: inputs are not dict as expected", settings=inputs)

        for k, v in six.iteritems(inputs):
            if k not in self.field_names:
                logger.info("trans_comp_lookup: key {0} doesn't exist in existing fields names: {1}".format(k, self.field_names))
                return None

        for row in self.rows:
            for k, v in six.iteritems(inputs):
                if row[k] != '*' and (row[k] != v if not self.case_insensitive else row[k].lower() != v.lower()):
                    break
            else:
                return row

        return None


class DefaultDict(object):
    def __init__(self, dct, case_insensitive):
        self.default = dct.get('*', None)
        self.case_insensitive = case_insensitive
        if case_insensitive:
            self.data = dict((k.lower(),  v) for k, v in six.iteritems(dct))
        else:
            self.data = dct

    def __getitem__(self, item):
        item = item.lower() if self.case_insensitive else item
        return self.data.get(item, self.default)

    def __contains__(self, item):
        item = item.lower() if self.case_insensitive else item
        return item in self.data or self.default is not None


class trans_comp_lookup(trans_comp_base):
    EXTERNAL_CACHE = {}

    def __init__(self, data, output_fields, sep=',', quote='"', lstrip=True, case_insensitive=True):
        if isinstance(data, dict):
            self.data = DefaultDict(data, case_insensitive)

            # init output fields
            if isinstance(output_fields, (six.binary_type, six.text_type) ):
                self.output_fields = [output_fields]
            elif isinstance(output_fields, Iterable):
                self.output_fields = []
                for x in output_fields:
                    if isinstance(output_fields, (six.binary_type, six.text_type)):
                        self.output_fields.append(x)
                    else:
                        raise SettingError(settings=output_fields)
            else:
                raise SettingError(settings=output_fields)

        elif isinstance(data, (six.binary_type, six.text_type) ):

            self.sig = (data, sep, quote, lstrip)

            if self.sig in self.EXTERNAL_CACHE:
                self.data = self.EXTERNAL_CACHE[self.sig]
            else:
                type_path = data.split("://")
                file_type = type_path[0] if len(type_path) == 2 else 'file'
                file_path = type_path[1] if len(type_path) == 2 else data
                if file_type != 'file':
                    raise SettingError(msg="trans_comp_lookup: unsupported file type", settings=data)
                if not os.path.isfile(file_path):
                    caller_frame_record = inspect.stack()[1]
                    module = inspect.getmodule(caller_frame_record[0])
                    file_path = os.path.sep.join([os.path.dirname(module.__file__), file_path])
                    if not os.path.isfile(file_path):
                        raise SettingError(msg="trans_comp_lookup: cannot locate the file path", settings=data)

                with open(file_path) as file:
                    reader = csv.DictReader(file, skipinitialspace=lstrip, delimiter=sep, quotechar=quote)
                    self.data = Table(reader, case_insensitive)

                # put into cache for re-use for other calling
                self.EXTERNAL_CACHE[self.sig] = self.data

            # init output fields
            self.output_fields = {}
            if isinstance(output_fields, (six.binary_type, six.text_type) ):
                self.output_fields[output_fields] = output_fields
            elif isinstance(output_fields, Iterable):
                for f in output_fields:
                    if isinstance(f, (six.binary_type, six.text_type)):
                        self.output_fields[f] = f
                    elif isinstance(f, (tuple, list) ) and len(f) == 2:
                        self.output_fields[f[0]] = f[1]
                    else:
                        raise SettingError(settings=output_fields)

                for f in self.output_fields:
                    assert f in self.data.field_names, SettingError(
                        msg="trans_comp_lookup: output field {0} doesn't exist in lookup table".format(f))
            else:
                raise SettingError(settings=output_fields)
        else:
            raise SettingError(settings=data)


    def __call__(self, event, inpt):
        if isinstance(self.data, DefaultDict):
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
                    f_v = event[i]

                    # set output value
                    for f_n in self.output_fields:
                        if f_v in self.data:
                            event[f_n] = self.data[f_v]
                        else:
                            logger.info('trans_comp_lookup: value {0} not exit in lookup {1}'.format(f_v, self.data))
            else:
                logger.error("trans_comp_lookup: unknown type of input field {0}".format(inpt))
        else:
            # lookup type
            if isinstance(inpt, (six.binary_type, six.text_type)):
                inpt = [inpt]

            if isinstance(inpt, Iterable):
                inpt_map = {}
                for i in inpt:
                    if not isinstance(i, (six.binary_type, six.text_type, list, tuple)):
                        logger.error('trans_comp_lookup: type of input field "{0}" is unknown'.format(i))
                        # must exit, or else skip it for lookup type
                        return event

                    if isinstance(i, (tuple, list)) and len(i) != 2:
                            logger.error('trans_comp_lookup: type of input field "{0}" is unsupported'.format(i))
                            # must exit, or else skip it for lookup type
                            return event

                    if isinstance(i, (six.binary_type, six.text_type)):
                        i = (i, i)

                    if i[0] not in event:
                        logger.info('trans_comp_lookup: event "{0}" doesn not contain field "{1}"'.format(event, i[0]))
                        # must exit, or else skip it for lookup type
                        return event

                    inpt_map[i[1]] = event[i[0]]

                row = self.data.get_row(inpt_map)
                if row is None:
                    logger.info('trans_comp_lookup: cannot find proper value for inpt "{0}" in event "{0}" doesn not contain field "{1}"'.format(inpt_map, event))
                    return event

                for f, f_new in six.iteritems(self.output_fields):
                    if f in row:
                        event[f_new] = row[f]
                    else:
                        logger.info("trans_comp_lookup: field {0} doesn't exit in lookup row {1}".format(f, row))

            else:
                logger.error("trans_comp_lookup: unknown type of input field {0}".format(inpt))

        return event

class trans_comp_json(trans_comp_base):
    def __init__(self, config):
        self.config = config

    def __call__(self, event, inpt):
        return event


