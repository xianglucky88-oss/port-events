from __future__ import annotations
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol
from .models import RawEvent


class EventSource(Protocol):
    def read(self) -> list[RawEvent]: ...


@dataclass(frozen=True)
class SourceBatch:
    """一组待采集批次：独立来源标识 + 批次标识 + 读取器。

    (source_id, batch_id) 是投递标识：相同标识视为同批重放；
    来源内容变化必须搭配新的 batch_id，否则按重放跳过。
    """
    source_id: str
    batch_id: str
    source: EventSource


class JsonEventSource:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def read(self) -> list[RawEvent]:
        payload = json.loads(self.path.read_text(encoding="utf-8"))
        return [RawEvent(**item) for item in payload]
