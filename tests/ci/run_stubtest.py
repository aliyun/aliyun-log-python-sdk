#!/usr/bin/env python
# encoding: utf-8

import os
import subprocess
import sys
import tempfile


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MODULES_FILE = os.path.join(BASE_DIR, 'stubtest_modules.txt')
ALLOWLIST_FILE = os.path.join(BASE_DIR, 'stubtest_allowlist.txt')


def _read_non_empty_lines(path):
    with open(path, 'r') as fh:
        return [line.strip() for line in fh if line.strip() and not line.lstrip().startswith('#')]


def main():
    modules = _read_non_empty_lines(MODULES_FILE)
    # Run outside the checkout so stubtest validates the installed package payload.
    os.chdir(tempfile.gettempdir())
    for module in modules:
        cmd = [sys.executable, '-m', 'mypy.stubtest', '--ignore-missing-stub']
        if os.path.exists(ALLOWLIST_FILE):
            cmd.extend(['--allowlist', ALLOWLIST_FILE])
        cmd.append(module)
        subprocess.check_call(cmd)


if __name__ == '__main__':
    main()
