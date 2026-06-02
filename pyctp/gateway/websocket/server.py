from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import Any

import websockets
from websockets.server import WebSocketServerProtocol

from pyctp.gateway.eventbus.bus import Event, EventBus
from pyctp.gateway.websocket.connection import ConnectionManager

logger = logging.getLogger(__name__)


class WebSocketServer:
    def __init__(self, host: str, port: int, bus: EventBus) -> None:
        self.host = host
        self.port = port
        self.bus = bus
        self._server: websockets.server.Serve | None = None
        self._connections = ConnectionManager()
        self._sender_task: asyncio.Task[None] | None = None
        self._started = False

    @property
    def connection_count(self) -> int:
        return self._connections.count()

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        self._server = await websockets.serve(self._handle_client, self.host, self.port)
        self._sender_task = asyncio.create_task(self._send_loop(), name="ws-send-loop")
        logger.info("websocket server started on %s:%s", self.host, self.port)

    async def stop(self) -> None:
        if self._sender_task:
            self._sender_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._sender_task
            self._sender_task = None
        for session in self._connections.all():
            await session.close()
        self._connections = ConnectionManager()
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        self._started = False

    async def broadcast(self, msg: str) -> None:
        for session in self._connections.all():
            await session.send(msg)

    async def send_to(self, conn_id: int, msg: str) -> None:
        session = self._connections.get(conn_id)
        if session is not None:
            await session.send(msg)

    def get_connection(self, conn_id: int) -> Any | None:
        return self._connections.get(conn_id)

    async def _handle_client(self, websocket: WebSocketServerProtocol) -> None:
        remote_addr = str(getattr(websocket, "remote_address", "unknown"))
        session = await self._connections.create(websocket, remote_addr)
        await self.bus.publish(Event(type="ws.connected", source="websocket", conn_id=session.conn_id, payload={"remote_addr": remote_addr}))

        try:
            async for message in websocket:
                if isinstance(message, bytes):
                    message = message.decode("utf-8", errors="ignore")
                await self.bus.publish(Event(type="ws.message", source="websocket", conn_id=session.conn_id, payload={"message": message}))
        except Exception as exc:
            logger.exception("websocket client error: %s", exc)
            await self.bus.publish(Event(type="ws.error", source="websocket", conn_id=session.conn_id, payload={"error": str(exc)}))
        finally:
            await self.bus.publish(Event(type="ws.disconnected", source="websocket", conn_id=session.conn_id, payload={"remote_addr": remote_addr}))
            self._connections.remove(session.conn_id)
            await session.close()

    async def _send_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(0.01)
                for session in self._connections.all():
                    while not session.send_queue.empty():
                        msg = await session.send_queue.get()
                        await session.websocket.send(msg)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("websocket send loop error")
