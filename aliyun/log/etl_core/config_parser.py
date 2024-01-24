import ast
import inspect
import logging
try:
    from collections.abc import Callable
except ImportError:
    from collections import Callable

import six
from functools import wraps

from .etl_util import re_full_match
from .settings import TransFnType, check_fn_type_by_name, builtin_macros

logger = logging.getLogger(__name__)


class ConfigParser(ast.NodeVisitor):
    @staticmethod
    def get_fn(fn, name):
        fn_type = check_fn_type_by_name(name)
        if fn_type == TransFnType.EVENT_NEW:
            return fn
        if fn_type == TransFnType.EVENT_UPDATE:
            @wraps(fn)
            def new_event_fn(event):
                r = fn(event)
                event.update(r)
                return event

            return new_event_fn

        return None

    @staticmethod
    def macro_check(name):
        for k, v in six.iteritems(builtin_macros):
            if re_full_match(k, name):
                return v

    def __init__(self, md):
        self.name_list = []
        self.fn_list = []
        self.md = md

    def visit_Name(self, node):
        if hasattr(self.md, node.id):
            obj = getattr(self.md, node.id)
            if isinstance(obj, Callable):
                fn = self.get_fn(obj, node.id)
                if fn:
                    logging.info("get name {0} which is a fn actually, add it".format(node.id))
                    self.name_list.append([node.lineno, fn])
            else:
                fn_map = self.macro_check(node.id)
                if fn_map is not None:
                    v = getattr(self.md, node.id)
                    self.name_list.append([node.lineno, fn_map(v)])
                else:
                    logging.info("get name {0} not in macro list, skip it".format(node.id))

    def generic_visit(self, node):
        if isinstance(node, ast.FunctionDef):
            obj = getattr(self.md, node.name, None)
            if inspect.isfunction(obj):
                fn = self.get_fn(obj, node.name)
                if fn:
                    self.fn_list.append([node.lineno, fn])

        ast.NodeVisitor.generic_visit(self, node)

    def parse(self):
        code = inspect.getsource(self.md)

        self.visit(ast.parse(code))
        return sorted(self.name_list + self.fn_list, key=lambda x: x[0])
