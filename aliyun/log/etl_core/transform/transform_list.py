import logging
from collections import Callable

import six
from ..trans_comp import REGEX
from .transform_base import transform_base
from ..etl_util import process_event

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
        return process_event(event, self.transform_list)