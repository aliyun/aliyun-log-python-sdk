"""
Transform list:
    v ==> [v]

1. dict
    1. fixed k-v
    dict-string (None means drop)

    2. fixed key - function
    dict-function (return None means drop)

    input: event
    result: field value

2. regex
    bi-string tuple

    ("data", r"(?P<f1>...)....(?P<f2>...)")

    "123 456": "\d+", "f1" ==> f1: 123
    "123 456": "\d+", ["f1", "f2"] ==> f1: 123   f2: 456
    "abc 123": "(\w+) (\d+)", ["f1", "f2"]  ==> f1: abc   f2: 456
    "abc 123": "(?P<f1>\w+) (?P<f2>\d+)" ==> f1: abc   f2: 456

    "abc:123 xyz:456": "(\w+):(\d+)", {"k_\1": "v_\2"} ==> k_abc: 123    k_xyz: 456

    Corner Case:
    "!abc 123# xyz 456": "(\w+) (\d+)", ["f1", "f2"], True  ==> f1: abc f2: 123


3. event function

    1. raw
    function

    input: event
    output: replaced_event

    2. drop
    DROP_EVENT(condition_list)
    DROP_FIELDS(condition_list)

    3. keep
    KEEP_EVENT(condition_list)
    KEEP_FIELDS(condition_list)

    4. rename
    RENAME_FIELDS(dict)


4. field function
    1. raw
    ("f1", lambda...)
    (["f1", "f2"], lambda...)

    input: user_input, event
    result: updated event

    2. XSV
    ("input_field", CSV("F1, F2, F3, F4"))
    ("input_field", TSV("F1, F2, F3, F4"))
    ("input_field", CSV(["F1", "F2", "F3", "F4"], SEP="\s*,\s*"))
    ("input_field", CSV(["F1", "F2", "F3", "F4"], SEP=",|\|") )

    3. lookup - dict
    # dct['*'] will match if any
    tuple/ list
    ("f1", LOOKUP({....}, "f2"))

    4. lookup - table
    # dct['*'] will match if any
    (["f1", 'f2', 'f3'], LOOKUP("./data.csv", ['out1', 'data2'])
    ([("f1", "f1_alias"), ("f2", "f2_alias"), 'f3'], LOOKUP("./data.csv", [('out1', 'out1_alias'), 'data2']) )

    5. JSON
    ("f1", JSON(filter="jmes_filter...", output="output_filed"))
    # f1: {"a": 1, "b": 2} =>
    ("f1", JSON(expand=True, level=1, prefix="f1.", suffix="", join="."))

    6. KV
    tuple/ list
    ("f1", KV(prefix="f1.", suffix="", sep="=")

"""

import logging
from collections import Callable

import six
from ..trans_comp import REGEX
from .transform_base import transform_base

logger = logging.getLogger(__name__)

__all__ = ['transform']


class transform(transform_base):
    def __init__(self, trans):
        if not isinstance(trans, list):
            self.trans = [trans]
        else:
            self.trans = trans

        self.transform_list = []
        for tr in self.trans:
            if isinstance(tr, Callable):
                self.transform_list.append(tr)
            elif isinstance(tr, (dict, )):
                def real_transform(event):
                    result = {}
                    for k, v in six.iteritems(tr):
                        if isinstance(v, Callable):
                            v = v(event)

                        if isinstance(v, (six.text_type, six.binary_type)):
                            result[k] = v
                        elif v is None:
                            if k in result:
                                del result[k]
                        else:
                            logger.warning("unknown type of transform value for key:{0} value:{1}".format(k, v))
                    event.update(result)
                    return event

                self.transform_list.append(real_transform)
            elif isinstance(tr, tuple):
                if len(tr) < 2 or len(tr) > 3:
                    logger.warning("invalid transform config: {0}".format(tr))
                    continue

                inpt, config = tr[0], tr[1]
                if isinstance(config, (six.text_type, six.binary_type)):
                    self.transform_list.append(lambda e: REGEX(*tr[1:])(e, inpt))
                elif isinstance(config, Callable):
                    self.transform_list.append(lambda e: config(e, inpt))
                else:
                    logger.warning("unknown transform config setting: {0}".format(config))
                    continue

    def __call__(self, event):
        for t in self.transform_list:
            event = t(event)
            if event is None:
                return None

        return event
