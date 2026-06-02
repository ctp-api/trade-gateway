from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass
from typing import Any

from websockets.server import WebSocketServerProtocol


@dataclass(slots=True)
class ConnectionSession:
    conn_id: int
    websocket: WebSocketServerProtocol
    remote_addr: str
    send_queue: asyncio.Queue[str]
    closed: bool = False

    async def send(self, msg: str) -> None:
        if self.closed:
            return
        await self.send_queue.put(msg)

    async def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        with contextlib.suppress(Exception):
            await self.websocket.close()


class ConnectionManager:
    def __init__(self) -> None:
        self._sessions: dict[int, ConnectionSession] = {}
        self._next_id = 1
        self._lock = asyncio.Lock()

    async def create(self, websocket: WebSocketServerProtocol, remote_addr: str) -> ConnectionSession:
        async with self._lock:
            conn_id = self._next_id
            self._next_id += 1
            session = ConnectionSession(
                conn_id=conn_id,
                websocket=websocket,
                remote_addr=remote_addr,
                send_queue=asyncio.Queue(maxsize=256),
            )
            self._sessions[conn_id] = session
            return session

    def get(self, conn_id: int) -> ConnectionSession | None:
        return self._sessions.get(conn_id)

    def remove(self, conn_id: int) -> None:
        self._sessions.pop(conn_id, None)

    def all(self) -> list[ConnectionSession]:
        return list(self._sessions.values())

    def count(self) -> int:
        return len(self._sessions)
