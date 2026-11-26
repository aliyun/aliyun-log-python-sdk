"""Load generated bindings for the installed Python 3 Protobuf runtime."""

from importlib import import_module

from google.protobuf import __version__


def load(module_name):
    major = __version__.split('.', 1)[0]
    if major not in ('3', '4', '5', '6', '7'):
        raise ImportError('Unsupported Protobuf version: {}'.format(__version__))
    return import_module('.v{}.{}'.format(major, module_name), __name__)
