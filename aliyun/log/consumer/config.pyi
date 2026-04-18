from typing import Any, Callable, Optional
from enum import Enum

class CursorPosition(Enum):
    BEGIN_CURSOR = 'BEGIN_CURSOR'
    END_CURSOR = 'END_CURSOR'
    SPECIAL_TIMER_CURSOR = 'SPECIAL_TIMER_CURSOR'

class ConsumerStatus(Enum):
    INITIALIZING = 'INITIALIZING'
    PROCESSING = 'PROCESSING'
    SHUTTING_DOWN = 'SHUTTING_DOWN'
    SHUTDOWN_COMPLETE = 'SHUTDOWN_COMPLETE'

class LogHubConfig:
    endpoint: str
    accessKeyId: str
    accessKey: str
    project: str
    logstore: str
    consumer_group_name: str
    consumer_name: str
    cursor_position: CursorPosition
    heartbeat_interval: int
    data_fetch_interval: int
    in_order: bool
    cursor_start_time: Any
    securityToken: Optional[str]
    max_fetch_log_group_size: int
    worker_pool_size: int
    shared_executor: Any
    consumer_group_time_out: int
    cursor_end_time: Any
    credentials_refresher: Optional[Callable[..., Any]]
    auth_version: str
    region: str
    query: Optional[str]
    accept_compress_type: Optional[str]
    processor: Optional[str]
    def __init__(self, endpoint: str, access_key_id: str, access_key: str, project: str, logstore: str, consumer_group_name: str, consumer_name: str, cursor_position: Optional[CursorPosition] = ..., heartbeat_interval: Optional[int] = ..., data_fetch_interval: Optional[int] = ..., in_order: bool = False, cursor_start_time: Any = ..., security_token: Optional[str] = ..., max_fetch_log_group_size: Optional[int] = ..., worker_pool_size: Optional[int] = ..., shared_executor: Any = ..., cursor_end_time: Any = ..., credentials_refresher: Optional[Callable[..., Any]] = ..., auth_version: str = ..., region: str = '', query: Optional[str] = ..., accept_compress_type: Optional[str] = ..., processor: Optional[str] = ...) -> None: ...
