from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

from pyctp.gateway.eventbus.bus import Event, EventBus

try:
    import websockets
    from websockets.server import WebSocketServerProtocol
except Exception:  # pragma: no cover
    websockets = None
    WebSocketServerProtocol = Any


@dataclass(slots=True)
class ConnectionSession:
    conn_id: int
    remote_addr: str
    websocket: Any
    connected_at: float = field(default_factory=lambda: asyncio.get_event_loop().time())
    last_active_at: float = field(default_factory=lambda: asyncio.get_event_loop().time())

    async def send(self, msg: str) -> None:
        await self.websocket.send(msg)
        self.touch()

    async def close(self, code: int = 1000, reason: str = "") -> None:
        await self.websocket.close(code=code, reason=reason)

    def touch(self) -> None:
        self.last_active_at = asyncio.get_event_loop().time()


class WebSocketServer:
    def __init__(self, host: str, port: int, bus: EventBus) -> None:
        self.host = host
        self.port = port
        self.bus = bus
        self._server: Any = None
        self._sessions: dict[int, ConnectionSession] = {}
        self._next_conn_id = 1

    async def start(self) -> None:
        if websockets is None:
            return
        self._server = await websockets.serve(self._handler, self.host, self.port)

    async def stop(self) -> None:
        for session in list(self._sessions.values()):
            try:
                await session.close()
            except Exception:
                pass
        self._sessions.clear()
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    async def broadcast(self, msg: str) -> None:
        for session in list(self._sessions.values()):
            try:
                await session.send(msg)
            except Exception:
                pass

    async def send_to(self, conn_id: int, msg: str) -> None:
        session = self._sessions.get(conn_id)
        if session is None:
            return
        try:
            await session.send(msg)
        except Exception:
            pass

    def get_connection(self, conn_id: int) -> ConnectionSession | None:
        return self._sessions.get(conn_id)

    def connection_count(self) -> int:
        return len(self._sessions)

    async def _handler(self, websocket: WebSocketServerProtocol) -> None:
        conn_id = self._next_conn_id
        self._next_conn_id += 1
        remote_addr = str(getattr(websocket, "remote_address", ""))
        session = ConnectionSession(conn_id=conn_id, remote_addr=remote_addr, websocket=websocket)
        self._sessions[conn_id] = session
        await self.bus.publish(Event(type="ws.connected", source="ws", conn_id=conn_id, payload={"remote_addr": remote_addr}))
        try:
            async for message in websocket:
                session.touch()
                await self.bus.publish(Event(type="ws.message", source="ws", conn_id=conn_id, payload={"message": message}))
        except Exception as exc:
            await self.bus.publish(Event(type="ws.error", source="ws", conn_id=conn_id, payload={"error": str(exc)}))
        finally:
            self._sessions.pop(conn_id, None)
            await self.bus.publish(Event(type="ws.disconnected", source="ws", conn_id=conn_id, payload={"remote_addr": remote_addr}))
