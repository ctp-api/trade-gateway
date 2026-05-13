from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from pathlib import Path
from typing import Awaitable, Callable

from pyctp.gateway.eventbus.bus import Event, EventBus
from pyctp.gateway.market.adapter import MarketFeedAdapter, PybindMdApiAdapter
from pyctp.gateway.market.models import MarketState, MarketStateMachine, Quote, QuoteStore
from pyctp.gateway.protocol import ProtocolCodec
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
        self._pending_subscriptions: set[str] = set()
        self._on_quotes: Callable[[list[Quote]], Awaitable[None]] | None = None
        self._started = False

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        self.state_machine.transition_to(MarketState.CONNECTING)
        await self.ws.start()
        await self.login()

    async def stop(self) -> None:
        self.state_machine.transition_to(MarketState.STOPPING)
        self.feed.close()
        await self.ws.stop()
        self.state_machine.transition_to(MarketState.STOPPED)

    async def login(self) -> None:
        if not self.state_machine.can_accept_login():
            return
        self.state_machine.transition_to(MarketState.LOGGING_IN)
        if self.config.md_front and self.config.user_name and self.config.broker_id:
            self.feed.connect(self.config.md_front, self.config.user_name, self.config.password, self.config.broker_id, self.config.auth_code, self.config.appid)
        self.feed.login()
        self.state_machine.transition_to(MarketState.READY)
        await self._restore_subscriptions()

    async def subscribe(self, *symbols: str) -> None:
        items = [symbol for symbol in symbols if symbol]
        if not items:
            return
        self._subscriptions.update(items)
        self._pending_subscriptions.update(items)
        self.feed.subscribe(items)
        self.state_machine.transition_to(MarketState.SUBSCRIBING)
        self.state_machine.transition_to(MarketState.READY)

    async def unsubscribe(self, *symbols: str) -> None:
        items = [symbol for symbol in symbols if symbol]
        if not items:
            return
        self._subscriptions.difference_update(items)
        self._pending_subscriptions.difference_update(items)
        self.feed.unsubscribe(items)
        self.state_machine.transition_to(MarketState.READY)

    def get_quote(self, symbol: str) -> Quote | None:
        return self.quotes.get(symbol)

    def get_quotes(self, symbols: list[str]) -> list[Quote]:
        return self.quotes.get_many(symbols)

    def is_subscribed(self, symbol: str) -> bool:
        return symbol in self._subscriptions

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

    def on_market_event(self, event: Event) -> None:
        if event.type == "market.quote.update":
            quote = event.payload.get("quote")
            if isinstance(quote, Quote):
                asyncio.create_task(self.on_tick(quote))

    @staticmethod
    def _serialize_quote_message(quote: Quote) -> str:
        import json
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
