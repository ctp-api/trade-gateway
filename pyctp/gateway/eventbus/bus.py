from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from time import time
from typing import Any


@dataclass(slots=True)
class Event:
    type: str
    source: str
    payload: dict[str, Any] = field(default_factory=dict)
    conn_id: int | None = None
    request_id: int | None = None
    created_at: float = field(default_factory=time)


class EventBus:
    def __init__(self) -> None:
        self._queue: asyncio.Queue[Event] = asyncio.Queue()
        self._loop: asyncio.AbstractEventLoop | None = None

    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop

    async def publish(self, event: Event) -> None:
        await self._queue.put(event)

    def publish_threadsafe(self, event: Event) -> None:
        if self._loop is not None and self._loop.is_running():
            self._loop.call_soon_threadsafe(self._queue.put_nowait, event)
        else:
            self._queue.put_nowait(event)

    async def get(self) -> Event:
        return await self._queue.get()

    def put_nowait(self, event: Event) -> None:
        self._queue.put_nowait(event)

    def queue(self) -> asyncio.Queue[Event]:
        return self._queue
