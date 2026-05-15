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
    tags: dict[str, Any] = field(default_factory=dict)


class EventBus:
    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
        self._subscribers: list[asyncio.Queue[Event]] = []
        self._default_queue: asyncio.Queue[Event] = asyncio.Queue()

    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop

    def subscribe(self) -> asyncio.Queue[Event]:
        queue: asyncio.Queue[Event] = asyncio.Queue()
        self._subscribers.append(queue)
        return queue

    async def publish(self, event: Event) -> None:
        await self._default_queue.put(event)
        for queue in list(self._subscribers):
            await queue.put(event)

    def publish_threadsafe(self, event: Event) -> None:
        if self._loop is not None and self._loop.is_running():
            self._loop.call_soon_threadsafe(self._default_queue.put_nowait, event)
            for queue in list(self._subscribers):
                self._loop.call_soon_threadsafe(queue.put_nowait, event)
        else:
            self._default_queue.put_nowait(event)
            for queue in list(self._subscribers):
                queue.put_nowait(event)

    async def get(self) -> Event:
        return await self._default_queue.get()

    def put_nowait(self, event: Event) -> None:
        self._default_queue.put_nowait(event)
        for queue in list(self._subscribers):
            queue.put_nowait(event)

    def queue(self) -> asyncio.Queue[Event]:
        return self._default_queue
