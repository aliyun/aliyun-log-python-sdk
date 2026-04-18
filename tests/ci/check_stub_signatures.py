#!/usr/bin/env python
# encoding: utf-8
"""
Verify that every method stubbed in a .pyi file has parameter names that match
the corresponding .py source. Only checks names present in BOTH files.
Exit 1 if any mismatches are found.
"""

import ast
import os
import sys


def _func_params(func_node):
    names = [a.arg for a in func_node.args.args]
    return names


def _collect_funcs(tree):
    """Return { qualified_name -> param_names } for all module-level and class-level functions."""
    result = {}
    for node in tree.body:
        if isinstance(node, ast.ClassDef):
            for item in node.body:
                if isinstance(item, ast.FunctionDef):
                    result[f"{node.name}.{item.name}"] = _func_params(item)
        elif isinstance(node, ast.FunctionDef):
            result[node.name] = _func_params(node)
    return result


def _parse(path):
    with open(path, 'r', encoding='utf-8') as fh:
        return ast.parse(fh.read(), filename=path)


def check_pair(py_path, pyi_path):
    py_funcs = _collect_funcs(_parse(py_path))
    pyi_funcs = _collect_funcs(_parse(pyi_path))

    mismatches = []
    for name, stub_params in pyi_funcs.items():
        if name not in py_funcs:
            continue
        stub_pos = [p for p in stub_params if p not in ('self', 'cls')]
        src_pos = [p for p in py_funcs[name] if p not in ('self', 'cls')]
        if stub_pos != src_pos:
            mismatches.append(f"  {name}: stub {stub_pos} != source {src_pos}")
    return mismatches


def main():
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    log_dir = os.path.join(repo_root, 'aliyun', 'log')

    all_mismatches = {}
    checked = 0

    for pyi_name in sorted(f for f in os.listdir(log_dir) if f.endswith('.pyi')):
        py_path = os.path.join(log_dir, pyi_name[:-1])
        if not os.path.exists(py_path):
            continue
        mismatches = check_pair(py_path, os.path.join(log_dir, pyi_name))
        checked += 1
        if mismatches:
            all_mismatches[pyi_name] = mismatches

    if all_mismatches:
        print('STUB SIGNATURE MISMATCHES FOUND:')
        for fname, issues in all_mismatches.items():
            print(f'\n{fname}:')
            for issue in issues:
                print(issue)
        total = sum(len(v) for v in all_mismatches.values())
        print(f'\n{total} mismatch(es) across {len(all_mismatches)} file(s).')
        sys.exit(1)
    else:
        print(f'OK — {checked} stub/source pair(s) checked, all signatures match.')


if __name__ == '__main__':
    main()
