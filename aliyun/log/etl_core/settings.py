from .transform import *
from enum import Enum

builtin_macros = {
    'KEEP_EVENT_.*': keep_event,
    'DROP_EVENT_.*': drop_event,
    'KEEP_FIELDS_.*': keep_fields,
    'DROP_FIELDS_.*': drop_fields,
    'RENAME_FIELDS_.*': rename_fields,
    'ALIAS_.*': rename_fields,
    'DISPATCH_EVENT_.*': dispatch_event,
    'TRANSFORM_EVENT_.*': transform_event,
    'KV_FIELDS_.*': extract_kv_fields
}

__all__ = ['KEEP_EVENT_',
           'DROP_EVENT_',
           'KEEP_FIELDS_',
           'DROP_FIELDS_',
           'RENAME_FIELDS_',
           'KV_FIELDS_',
           'ALIAS_',
           'DISPATCH_EVENT_',
           'TRANSFORM_EVENT_']

for key in builtin_macros.keys():
    globals()[key[:-2]] = "Use this prefix to auto call the function: {0}".format(builtin_macros[key])


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
