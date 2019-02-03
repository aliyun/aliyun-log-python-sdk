import six

from .transform_base import transform_base
from ..etl_util import get_re_full_match
from ..exceptions import SettingError
from ..trans_comp import KV
from ..etl_util import support_event_list_simple, u

__all__ = ["simple_drop", "simple_keep", 'drop_fields', "keep_fields", "rename_fields", 'extract_kv_fields',
           'DROP', 'KEEP', 'ALIAS', 'RENAME', 'DROP_F', 'KEEP_F', 'KV_F']


class simple_drop(transform_base):
    @support_event_list_simple
    def __call__(self, event):
        return None


class simple_keep(transform_base):
    @support_event_list_simple
    def __call__(self, event):
        return event


class drop_fields(transform_base):
    def __init__(self, config):
        if isinstance(config, (six.text_type, six.binary_type)):
            self.check = get_re_full_match(config)
        elif isinstance(config, list):  # string list
            checks = [get_re_full_match(c) for c in config]
            self.check = lambda k: any(ck(k) for ck in checks)
        else:
            raise SettingError(None, "keep_fields setting {0} is not supported".format(config))

    @support_event_list_simple
    def __call__(self, event):
        return dict((k, v) for k, v in six.iteritems(event) if not self.check(k))


class keep_fields(transform_base):
    def __init__(self, config):
        config = u(config)

        if isinstance(config, (six.text_type, six.binary_type)):
            self.check = get_re_full_match(config)
        elif isinstance(config, list):  # string list
            checks = [get_re_full_match(c) for c in config]
            self.check = lambda k: any(ck(k) for ck in checks)
        else:
            raise SettingError(None, "keep_fields setting {0} is not supported".format(config))

    @support_event_list_simple
    def __call__(self, event):
        return dict((k, v) for k, v in six.iteritems(event) if self.check(k))


class rename_fields(transform_base):
    def __init__(self, config):
        config = u(config)

        if isinstance(config, (dict, )):
            try:
                config_ptn = [(get_re_full_match(k), v) for k, v in six.iteritems(config)]

                def check(k):
                    for c, new_k in config_ptn:
                        if c(k):
                            return new_k
                    return k

                self.new_name = check
            except Exception as ex:
                raise SettingError(ex, "rename setting {0} is not invalid".format(config))
        elif config is None or config == "":
            self.new_name = lambda k: k
        else:
            raise SettingError(None, "rename setting {0} is not supported, should be dict type".format(config))

    @support_event_list_simple
    def __call__(self, event):
        return dict((self.new_name(k), v) for k, v in six.iteritems(event))


class extract_kv_fields(transform_base):
    def __init__(self, config):
        config = u(config)

        if isinstance(config, (six.text_type, six.binary_type)):
            self.check = get_re_full_match(config)
        elif isinstance(config, list):  # string list
            checks = [get_re_full_match(c) for c in config]
            self.check = lambda k: any(ck(k) for ck in checks)
        else:
            raise SettingError(None, "extract_kv setting {0} is not supported".format(config))

    @support_event_list_simple
    def __call__(self, event):
        fields_list = [k for k, v in six.iteritems(event) if self.check(k)]
        return KV(event, fields_list)


DROP = simple_drop()
KEEP = simple_keep()

ALIAS, RENAME = rename_fields, rename_fields
DROP_F = drop_fields
KEEP_F = keep_fields
KV_F = extract_kv_fields
