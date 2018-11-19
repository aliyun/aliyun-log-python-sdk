
from .transform import *
# from .transform.transform_base import transform_base
# from .trans_comp.trans_base import trans_comp_base
# from collections import Callable
from enum import Enum
# import re

builtin_macros = {
    'KEEP_EVENT_.*': keep_event,
    'DROP_EVENT_.*': drop_event,
    'KEEP_FIELDS_.*': keep_fields,
    'DROP_FIELDS_.*': drop_fields,
    'RENAME_FIELDS_.*': rename_fields,
    'ALIAS_.*': rename_fields,
    'DISPATCH_EVENT_.*': dispatch_event,
    'TRANSFORM_EVENT_.*': transform_event
}





class TransFnType(Enum):
    EVENT_NEW = "EVENT_NEW",
    EVENT_UPDATE = 'EVENT_UPDATE',
    # TRANS_COMP = 'TRANS_COMP'
    UNKNOWN = 'UNKNOWN'


def check_fn_type_by_name(name):
    # if isinstance(fn, transform_base):
    #     return TransFnType.EVENT_NEW
    # elif isinstance(fn, trans_comp_base):
    #     return TransFnType.TRANSFORM_COMP
    # if not isinstance(fn, Callable):
    #     fn_name = getattr(fn, '__name__', getattr(fn, "func_name", ""))
    if name.startswith("sls_en_"):
        return TransFnType.EVENT_NEW
    elif name.startswith("sls_eu_"):
        return TransFnType.EVENT_UPDATE

    return TransFnType.UNKNOWN

