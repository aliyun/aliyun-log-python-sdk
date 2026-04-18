# -*- coding: utf-8 -*-
from typing import Any, Dict

from .logresponse import LogResponse

class PutLogsResponse(LogResponse):
    def __init__(self, header: Dict[str, Any], resp: str = ...) -> None: ...
    def log_print(self) -> None: ...
