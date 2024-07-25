import sys

MAX_PYTHON_VERSION = (3, 0)

if sys.version_info >= MAX_PYTHON_VERSION:
    raise RuntimeError("This package requires only works on Python 2. "
                       "You are using Python {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}.")