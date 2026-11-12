from __future__ import annotations
import json
from pathlib import Path
from typing import Protocol
from .models import RawEvent


class EventSource(Protocol):
    def read(self) -> list[RawEvent]: ...


class JsonEventSource:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def read(self) -> list[RawEvent]:
        payload = json.loads(self.path.read_text(encoding="utf-8"))
        return [RawEvent(**item) for item in payload]
