import logging
from typing import Any, Dict, MutableMapping, Optional, Sequence, Tuple
from threading import Thread
from .config import LogHubConfig as LogHubConfig

class ConsumerWorkerLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg: Any, kwargs: MutableMapping[str, Any]) -> Tuple[Any, MutableMapping[str, Any]]: ...

class ConsumerWorker(Thread):
    make_processor: Any
    process_args: Sequence[Any]
    process_kwargs: Dict[str, Any]
    option: LogHubConfig
    consumer_client: Any
    shut_down_flag: bool
    logger: logging.LoggerAdapter
    shard_consumers: Dict[Any, Any]
    last_owned_consumer_finish_time: float
    heart_beat: Any
    own_executor: bool
    def __init__(self, make_processor: Any, consumer_option: LogHubConfig, args: Optional[Sequence[Any]] = ..., kwargs: Optional[Dict[str, Any]] = ...) -> None: ...
    @property
    def executor(self) -> Any: ...
    def run(self) -> None: ...
    def start(self, join: bool = False) -> None: ...
    def shutdown_and_wait(self) -> None: ...
    def clean_shard_consumer(self, owned_shards: Sequence[Any]) -> None: ...
    def shutdown(self) -> None: ...
