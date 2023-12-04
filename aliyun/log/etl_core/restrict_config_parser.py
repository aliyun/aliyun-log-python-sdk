import ast
import logging
import six
import sys


TRUST_AST_TYPES = (ast.Call, ast.Module, ast.List, ast.Tuple, ast.Dict, ast.Name, ast.Num, ast.Str,
                   ast.Assign, ast.Load)

if sys.version_info[:2] == (3, 3):
    TRUST_AST_TYPES = TRUST_AST_TYPES + (ast.Bytes,)
elif six.PY3:
    TRUST_AST_TYPES = TRUST_AST_TYPES + (ast.Bytes, ast.NameConstant)


class InvalidETLConfig(Exception):
    pass


builtin_macros = [
    'KEEP_EVENT_',
    'DROP_EVENT_',
    'KEEP_FIELDS_',
    'DROP_FIELDS_',
    'RENAME_FIELDS_',
    'ALIAS_',
    'DISPATCH_EVENT_',
    'TRANSFORM_EVENT_',
    'KV_FIELDS_'
]

built_in_fns = ['V', 'JSON', 'CSV', 'REGEX', 'EMPTY', 'NO_EMPTY', 'DROP_F', 'KV', 'TSV', 'PSV', 'LOOKUP', 'SPLIT', 'ZIP']

built_in_ids = ['KV', 'ANY', 'ALL', 'F_TIME', 'F_META', 'F_TAGS', 'SPLIT', 'JSON', 'True', 'False', 'None']


logger = logging.getLogger(__name__)


class RestrictConfigParser(ast.NodeVisitor):
    def visit_ImportFrom(self, node):
        if node.module == 'aliyun.log.etl_core' and len(node.names) == 1 and node.names[0].name == '*':
            logger.info("[Passed] import detected: from aliyun.log.etl_core import *")
        else:
            raise InvalidETLConfig("unknown import: {0}".format(node.module))

    def visit_Call(self, node):
        if isinstance(node.func, ast.Name):
            if isinstance(node.func.ctx, ast.Load) and node.func.id in built_in_fns:
                logger.info("[Passed] known call detected")
            else:
                raise InvalidETLConfig("unknown call id detected: {0}".format(node.func.id))
        else:
            raise InvalidETLConfig("unknown call type detected: {0}".format(node.func))

    def visit_Name(self, node):
        if isinstance(node.ctx, ast.Store):
            for p in builtin_macros:
                if node.id.startswith(p):
                    logger.info('[Passed] assign detected: ', node.id)
                    break
            else:
                raise InvalidETLConfig('unknown assign detected: ', node.id)
        elif isinstance(node.ctx, ast.Load):
            if node.id in built_in_ids:
                logger.info(' [Passed] assigned name:', node.id)
            else:
                raise InvalidETLConfig('unknown load detected: ', node.id)
        else:
            raise InvalidETLConfig("unknown Name: {0}".format(node.id))

    def generic_visit(self, node):
        if isinstance(node, TRUST_AST_TYPES):
            logger.info("... known type detected: ", type(node))
        else:
            raise InvalidETLConfig("unknown type detected: {0}".format(type(node)))

        ast.NodeVisitor.generic_visit(self, node)

    def parse(self, code):
        self.visit(ast.parse(code))
