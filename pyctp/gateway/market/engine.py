from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Awaitable, Callable

from pyctp.gateway.eventbus.bus import Event, EventBus
from pyctp.gateway.market.adapter import MarketFeedAdapter, PybindMdApiAdapter
from pyctp.gateway.market.models import MarketState, MarketStateMachine, Quote, QuoteStore
from pyctp.gateway.protocol import ProtocolCodec
from pyctp.gateway.protocol.types import MarketLoginRequest, WsRequest
from pyctp.gateway.websocket import WebSocketServer


@dataclass(slots=True)
class MarketConfig:
    host: str = "0.0.0.0"
    port: int = 7789
    log_level: str = "INFO"
    data_dir: Path = field(default_factory=lambda: Path("./data"))
    md_front: str = ""
    broker_id: str = ""
    user_name: str = ""
    password: str = ""
    auth_code: str = ""
    appid: str = ""


class MarketEngine:
    def __init__(self, bus: EventBus, feed: MarketFeedAdapter | None, config: MarketConfig, ws: WebSocketServer | None = None) -> None:
        self.bus = bus
        self.feed = feed or MarketFeedAdapter(PybindMdApiAdapter(bus=bus), bus=bus)
        self.config = config
        self.ws = ws or WebSocketServer(config.host, config.port, bus)
        self.codec = ProtocolCodec()
        self.state_machine = MarketStateMachine()
        self.quotes = QuoteStore()
        self._subscriptions: set[str] = set()
        self._conn_subscriptions: dict[int, set[str]] = {}
        self._pending_subscriptions: set[str] = set()
        self._on_quotes: Callable[[list[Quote]], Awaitable[None]] | None = None
        self._started = False
        self._router_task: asyncio.Task[None] | None = None
        self._event_queue: asyncio.Queue[Event] | None = None

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        self.state_machine.transition_to(MarketState.INIT)
        await self.ws.start()
        self._event_queue = self.bus.subscribe()
        self._router_task = asyncio.create_task(self._event_router_loop(), name="market-event-router")

    async def stop(self) -> None:
        self.state_machine.transition_to(MarketState.STOPPING)
        self.feed.close()
        if self._router_task is not None:
            self._router_task.cancel()
            try:
                await self._router_task
            except asyncio.CancelledError:
                pass
            self._router_task = None
        await self.ws.stop()
        self.state_machine.transition_to(MarketState.STOPPED)

    async def login(self, req: MarketLoginRequest | None = None) -> None:
        if not self.state_machine.can_accept_login():
            return
        login_req = req or MarketLoginRequest(
            user_name=self.config.user_name,
            password=self.config.password,
            broker_id=self.config.broker_id,
            front=self.config.md_front,
            auth_code=self.config.auth_code,
            appid=self.config.appid,
        )
        self.state_machine.transition_to(MarketState.LOGGING_IN)
        if login_req.front and login_req.user_name and login_req.broker_id:
            self.feed.connect(login_req.front, login_req.user_name, login_req.password, login_req.broker_id, login_req.auth_code, login_req.appid)
        self.feed.login()
        self.state_machine.transition_to(MarketState.READY)
        await self._restore_subscriptions()

    async def subscribe(self, *symbols: str) -> None:
        items = [self._normalize_symbol(symbol) for symbol in symbols if symbol]
        if not items:
            return
        self._subscriptions.update(items)
        self._pending_subscriptions.update(items)
        self.feed.subscribe(items)
        self.state_machine.transition_to(MarketState.SUBSCRIBING)
        self.state_machine.transition_to(MarketState.READY)

    async def unsubscribe(self, *symbols: str) -> None:
        items = [self._normalize_symbol(symbol) for symbol in symbols if symbol]
        if not items:
            return
        self._subscriptions.difference_update(items)
        self._pending_subscriptions.difference_update(items)
        self.feed.unsubscribe(items)
        self.state_machine.transition_to(MarketState.READY)

    def get_quote(self, symbol: str) -> Quote | None:
        return self.quotes.get(self._normalize_symbol(symbol))

    def get_quotes(self, symbols: list[str]) -> list[Quote]:
        return self.quotes.get_many([self._normalize_symbol(symbol) for symbol in symbols])

    def is_subscribed(self, symbol: str) -> bool:
        return self._normalize_symbol(symbol) in self._subscriptions

    def set_on_quotes(self, callback: Callable[[list[Quote]], Awaitable[None]]) -> None:
        self._on_quotes = callback

    async def on_tick(self, quote: Quote) -> None:
        self.quotes.update(quote)
        if self._on_quotes is not None:
            await self._on_quotes([quote])
        await self.bus.publish(Event(type="market.quote.update", source="market", payload={"quote": quote}, tags={"symbol": quote.symbol}))
        await self.ws.broadcast(self._serialize_quote_message(quote))

    async def _restore_subscriptions(self) -> None:
        if self._subscriptions:
            self.feed.subscribe(sorted(self._subscriptions))
            self._pending_subscriptions.update(self._subscriptions)

    async def _event_router_loop(self) -> None:
        assert self._event_queue is not None
        while True:
            event = await self._event_queue.get()
            if event.type == "market.quote.update":
                quote = event.payload.get("quote")
                if isinstance(quote, Quote):
                    await self._push_quote_to_clients(quote)
            elif event.type == "ws.message":
                await self._handle_ws_message(event)
            elif event.type == "ws.connected":
                conn_id = event.conn_id or 0
                await self.ws.send_to(conn_id, self.codec.build_notify(0, "market ready"))
                await self._restore_client_subscriptions(conn_id)
            elif event.type == "ws.disconnected":
                conn_id = event.conn_id or 0
                self._conn_subscriptions.pop(conn_id, None)

    async def _handle_ws_message(self, event: Event) -> None:
        conn_id = event.conn_id or 0
        raw = str(event.payload.get("message", ""))
        try:
            req = self.codec.parse_request(raw, conn_id=conn_id)
        except Exception as exc:
            await self.ws.send_to(conn_id, self.codec.dumps({"aid": "error", "ok": False, "code": 400, "msg": str(exc), "conn_id": conn_id}))
            return
        request_id = req.request_id or 1
        if req.aid == "market_login":
            login = self.codec.parse_market_login(req)
            await self.login(login)
            await self.ws.send_to(conn_id, self.codec.dumps({"aid": "market_login", "ok": True, "code": 0, "msg": "login accepted", "data": {"status": "ready"}, "request_id": request_id, "conn_id": conn_id}))
            await self._restore_client_subscriptions(conn_id)
        elif req.aid == "market_subscribe":
            symbols = self._extract_symbols(req)
            self.attach_client_subscription(conn_id, symbols)
            await self.subscribe(*symbols)
            await self.ws.send_to(conn_id, self.codec.dumps({"aid": "market_subscribe", "ok": True, "code": 0, "msg": "accepted", "data": {"symbols": symbols}, "request_id": request_id, "conn_id": conn_id}))
            await self._restore_client_subscriptions(conn_id)
        elif req.aid == "market_unsubscribe":
            symbols = self._extract_symbols(req)
            self.detach_client_subscription(conn_id, symbols)
            await self.unsubscribe(*symbols)
            await self.ws.send_to(conn_id, self.codec.dumps({"aid": "market_unsubscribe", "ok": True, "code": 0, "msg": "accepted", "data": {"symbols": symbols}, "request_id": request_id, "conn_id": conn_id}))

    async def _push_quote_to_clients(self, quote: Quote) -> None:
        message = self._serialize_quote_message(quote)
        matched = False
        for conn_id, symbols in self._conn_subscriptions.items():
            if quote.symbol in symbols:
                await self.ws.send_to(conn_id, message)
                matched = True
        if not matched and not self._conn_subscriptions:
            await self.ws.broadcast(message)

    def attach_client_subscription(self, conn_id: int, symbols: list[str]) -> None:
        normalized = {self._normalize_symbol(symbol) for symbol in symbols if symbol}
        self._conn_subscriptions.setdefault(conn_id, set()).update(normalized)
        self._subscriptions.update(normalized)

    def detach_client_subscription(self, conn_id: int, symbols: list[str]) -> None:
        normalized = {self._normalize_symbol(symbol) for symbol in symbols if symbol}
        current = self._conn_subscriptions.get(conn_id)
        if current is not None:
            current.difference_update(normalized)
            if not current:
                self._conn_subscriptions.pop(conn_id, None)

    async def _restore_client_subscriptions(self, conn_id: int) -> None:
        symbols = self._conn_subscriptions.get(conn_id)
        if symbols:
            await self.ws.send_to(conn_id, self.codec.dumps({"aid": "market_subscribe", "ok": True, "code": 0, "msg": "restored", "data": {"symbols": sorted(symbols)}, "request_id": None, "conn_id": conn_id}))

    def on_market_event(self, event: Event) -> None:
        if event.type == "market.quote.update":
            quote = event.payload.get("quote")
            if isinstance(quote, Quote):
                asyncio.create_task(self.on_tick(quote))
        elif event.type == "ws.connected":
            conn_id = event.conn_id or 0
            asyncio.create_task(self._restore_client_subscriptions(conn_id))

    def _extract_symbols(self, req: WsRequest) -> list[str]:
        data = req.raw.get("data") if isinstance(req.raw, dict) else {}
        if not isinstance(data, dict):
            return []
        symbols = data.get("symbols") or data.get("instrument_ids") or []
        if isinstance(symbols, list):
            return [self._normalize_symbol(str(s)) for s in symbols if s]
        if isinstance(symbols, str):
            return [self._normalize_symbol(symbols)]
        return []

    @staticmethod
    def _normalize_symbol(symbol: str) -> str:
        return symbol.strip().upper()

    @staticmethod
    def _serialize_quote_message(quote: Quote) -> str:
        payload = {
            "aid": "market_quote",
            "ok": True,
            "code": 0,
            "msg": "ok",
            "data": {"quote": quote.__dict__ if hasattr(quote, "__dict__") else quote},
            "request_id": None,
            "conn_id": None,
        }
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
