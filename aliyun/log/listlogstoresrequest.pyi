# -*- coding: utf-8 -*-
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, Union

from .logrequest import LogRequest

class ListLogstoresRequest(LogRequest):
    def __init__(self, project: Optional[str] = ...) -> None: ...
