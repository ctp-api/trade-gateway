from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Awaitable, Callable

from pyctp.gateway.eventbus.bus import Event, EventBus
from pyctp.gateway.market.adapter import MarketFeedAdapter, PybindMdApiAdapter
from pyctp.gateway.market.models import MarketState, MarketStateMachine, Quote, QuoteStore
from pyctp.gateway.protocol import ProtocolCodec
from pyctp.gateway.protocol.types import MarketLoginRequest, WsRequest
from pyctp.gateway.websocket import WebSocketServer

logger = logging.getLogger(__name__)


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
        self._pending_login_requests: dict[int, int] = {}
        self._pending_login_conn_by_reqid: dict[int, int] = {}
        self._pending_login_conn_id: int | None = None
        self._pending_login_request_id: int | None = None
        self._login_timeout_task: asyncio.Task[None] | None = None
        self._on_quotes: Callable[[list[Quote]], Awaitable[None]] | None = None
        self._started = False
        self._router_task: asyncio.Task[None] | None = None
        self._event_queue: asyncio.Queue[Event] | None = None

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        self.state_machine.transition_to(MarketState.INIT)
        logger.info("market engine starting host=%s port=%s md_front=%s broker_id=%s", self.config.host, self.config.port, self.config.md_front, self.config.broker_id)
        await self.ws.start()
        self._event_queue = self.bus.subscribe()
        self._router_task = asyncio.create_task(self._event_router_loop(), name="market-event-router")

    async def stop(self) -> None:
        self.state_machine.transition_to(MarketState.STOPPING)
        logger.info("market engine stopping")
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

    async def login(self, req: MarketLoginRequest | None = None, conn_id: int | None = None) -> bool:
        if not self.state_machine.can_accept_login() and conn_id is None:
            logger.warning("market login rejected by state=%s", self.state_machine.get_state())
            return False
        login_req = req or MarketLoginRequest(
            user_name=self.config.user_name,
            password=self.config.password,
            broker_id=self.config.broker_id,
            front=self.config.md_front,
            auth_code=self.config.auth_code,
            appid=self.config.appid,
        )
        if conn_id is not None:
            self._pending_login_requests[conn_id] = self._pending_login_requests.get(conn_id, 0)
            self._pending_login_conn_id = conn_id
        logger.info("market login request front=%s broker_id=%s user_name=%s appid=%s", login_req.front, login_req.broker_id, login_req.user_name, login_req.appid)
        self.state_machine.transition_to(MarketState.LOGGING_IN)
        try:
            if login_req.front and login_req.user_name and login_req.broker_id:
                logger.info("market connecting front=%s", login_req.front)
                self.feed.connect(login_req.front, login_req.user_name, login_req.password, login_req.broker_id, login_req.auth_code, login_req.appid)
                logger.info("market connect returned front=%s", login_req.front)
                self._start_login_timeout_watchdog()
            else:
                logger.warning("market login missing connect params front=%s broker_id=%s user_name=%s", login_req.front, login_req.broker_id, login_req.user_name)
                return False
        except Exception as exc:
            self._cancel_login_timeout_watchdog()
            self._pending_login_conn_id = None
            self._pending_login_request_id = None
            self.state_machine.transition_to(MarketState.INIT)
            logger.exception("market connect failed: %s", exc)
            return False
        return True

    async def subscribe(self, *symbols: str) -> None:
        items = [self._normalize_symbol(symbol) for symbol in symbols if symbol]
        if not items:
            return
        logger.info("market subscribe request symbols=%s", items)
        self._subscriptions.update(items)
        self._pending_subscriptions.update(items)
        self.feed.subscribe(items)
        self.state_machine.transition_to(MarketState.SUBSCRIBING)
        self.state_machine.transition_to(MarketState.READY)

    async def unsubscribe(self, *symbols: str) -> None:
        items = [self._normalize_symbol(symbol) for symbol in symbols if symbol]
        if not items:
            return
        logger.info("market unsubscribe request symbols=%s", items)
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

    def _start_login_timeout_watchdog(self) -> None:
        self._cancel_login_timeout_watchdog()
        self._login_timeout_task = asyncio.create_task(self._login_timeout_after(10.0), name="market-login-timeout")

    def _cancel_login_timeout_watchdog(self) -> None:
        if self._login_timeout_task is not None:
            self._login_timeout_task.cancel()
            self._login_timeout_task = None

    async def _login_timeout_after(self, timeout_seconds: float) -> None:
        try:
            await asyncio.sleep(timeout_seconds)
            if self.state_machine.get_state() == MarketState.LOGGING_IN:
                conn_id = self._pending_login_conn_id or 0
                request_id = self._pending_login_request_id or self._pending_login_requests.get(conn_id, 0)
                logger.warning("market login timeout after %.1fs conn_id=%s request_id=%s", timeout_seconds, conn_id, request_id)
                self.state_machine.transition_to(MarketState.INIT)
                await self._send_market_response(conn_id, "market_login", request_id, False, "login timeout", {"status": "timeout", "timeout_seconds": timeout_seconds}, code=504)
                self._pending_login_conn_id = None
                self._pending_login_request_id = None
        except asyncio.CancelledError:
            return

    async def _restore_subscriptions(self) -> None:
        if self._subscriptions:
            logger.info("market restoring subscriptions=%s", sorted(self._subscriptions))
            self.feed.subscribe(sorted(self._subscriptions))
            self._pending_subscriptions.update(self._subscriptions)

    async def _send_market_response(self, conn_id: int, aid: str, request_id: int | None, ok: bool, msg: str, data: dict[str, object] | None = None, code: int | None = None) -> None:
        payload = {
            "aid": aid,
            "ok": ok,
            "code": 0 if code is None and ok else (code if code is not None else 500),
            "msg": msg,
            "data": data or {},
            "request_id": request_id,
            "conn_id": conn_id,
        }
        logger.info("market response conn_id=%s aid=%s ok=%s msg=%s data=%s", conn_id, aid, ok, msg, data)
        await self.ws.send_to(conn_id, self.codec.dumps(payload))

    async def _event_router_loop(self) -> None:
        assert self._event_queue is not None
        logger.info("market event router started")
        while True:
            try:
                event = await self._event_queue.get()
                logger.debug("market event type=%s conn_id=%s request_id=%s", event.type, event.conn_id, event.request_id)
                if event.type == "market.quote.update":
                    quote = event.payload.get("quote")
                    if isinstance(quote, Quote):
                        await self._push_quote_to_clients(quote)
                elif event.type == "ws.message":
                    await self._handle_ws_message(event)
                elif event.type == "ws.connected":
                    conn_id = event.conn_id or 0
                    await self.ws.send_to(conn_id, self.codec.build_notify(0, "market ready"))
                elif event.type == "market.front_connected":
                    logger.info("market front connected")
                elif event.type == "market.login_rsp":
                    error = event.payload.get("error") or {}
                    ok = int(error.get("ErrorID", 0)) == 0
                    reqid = int(event.request_id or 0)
                    conn_id = self._pending_login_conn_by_reqid.pop(reqid, self._pending_login_conn_id or event.conn_id or 0)
                    if self._pending_login_request_id == reqid:
                        self._pending_login_request_id = None
                    if ok:
                        logger.info("market login rsp success reqid=%s conn_id=%s", reqid, conn_id)
                        self._cancel_login_timeout_watchdog()
                        self.state_machine.transition_to(MarketState.READY)
                        await self._send_market_response(conn_id, "market_login", reqid, True, "login success", {"status": "ready"})
                        await self._restore_subscriptions()
                        await self._restore_client_subscriptions(conn_id)
                    else:
                        logger.warning("market login rsp error=%s", error)
                        self._cancel_login_timeout_watchdog()
                        self.state_machine.transition_to(MarketState.INIT)
                        await self._send_market_response(conn_id, "market_login", reqid, False, str(error.get("ErrorMsg", "login failed")), {"error": error}, code=int(error.get("ErrorID", 500)) or 500)
                    self._pending_login_conn_id = None
                elif event.type == "ws.disconnected":
                    conn_id = event.conn_id or 0
                    logger.info("market ws disconnected conn_id=%s", conn_id)
                    self._conn_subscriptions.pop(conn_id, None)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.exception("market event router error: %s", exc)
                await self.bus.publish(Event(type="market.error", source="market", payload={"error": str(exc)}))

    async def _handle_ws_message(self, event: Event) -> None:
        conn_id = event.conn_id or 0
        raw = str(event.payload.get("message", ""))
        logger.info("market ws message conn_id=%s raw=%s", conn_id, raw)
        try:
            req = self.codec.parse_request(raw, conn_id=conn_id)
        except Exception as exc:
            logger.exception("market request parse failed conn_id=%s", conn_id)
            await self._send_market_response(conn_id, "error", None, False, str(exc), code=400)
            return
        request_id = req.request_id or 1
        if req.aid == "market_login":
            login = self.codec.parse_market_login(req)
            ok = await self.login(login, conn_id=conn_id)
            if ok:
                self._pending_login_requests[conn_id] = request_id
                self._pending_login_conn_by_reqid[request_id] = conn_id
                self._pending_login_request_id = request_id
                await self._send_market_response(conn_id, "market_login", request_id, True, "login connecting", {"status": "connecting"})
            else:
                self._pending_login_requests.pop(conn_id, None)
                if self._pending_login_request_id == request_id:
                    self._pending_login_request_id = None
                await self._send_market_response(conn_id, "market_login", request_id, False, "login rejected", {"status": str(self.state_machine.get_state())}, code=500)
        elif req.aid == "market_subscribe":
            symbols = self._extract_symbols(req)
            logger.info("market subscribe conn_id=%s symbols=%s", conn_id, symbols)
            self.attach_client_subscription(conn_id, symbols)
            await self.subscribe(*symbols)
            await self._send_market_response(conn_id, "market_subscribe", request_id, True, "accepted", {"symbols": symbols})
            await self._restore_client_subscriptions(conn_id)
        elif req.aid == "market_unsubscribe":
            symbols = self._extract_symbols(req)
            logger.info("market unsubscribe conn_id=%s symbols=%s", conn_id, symbols)
            self.detach_client_subscription(conn_id, symbols)
            await self.unsubscribe(*symbols)
            await self._send_market_response(conn_id, "market_unsubscribe", request_id, True, "accepted", {"symbols": symbols})
        else:
            logger.warning("market unsupported aid=%s conn_id=%s", req.aid, conn_id)
            await self._send_market_response(conn_id, req.aid, request_id, False, f"unsupported aid: {req.aid}", code=404)

    async def _push_quote_to_clients(self, quote: Quote) -> None:
        message = self._serialize_quote_message(quote)
        matched = False
        for conn_id, symbols in self._conn_subscriptions.items():
            if quote.symbol in symbols:
                logger.info("market pushing quote conn_id=%s symbol=%s", conn_id, quote.symbol)
                await self.ws.send_to(conn_id, message)
                matched = True
        if not matched and not self._conn_subscriptions:
            logger.info("market broadcasting quote symbol=%s", quote.symbol)
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
            await self._send_market_response(conn_id, "market_subscribe", None, True, "restored", {"symbols": sorted(symbols)}, code=0)

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
        symbol = symbol.strip()
        if "." not in symbol:
            return symbol.lower() if symbol[:2].isalpha() else symbol
        exchange_id, instrument_id = symbol.split(".", 1)
        exchange_id = exchange_id.strip().upper()
        instrument_id = instrument_id.strip()
        if exchange_id == "SHFE":
            instrument_id = instrument_id.lower()
        return f"{exchange_id}.{instrument_id}"

    @staticmethod
    def _serialize_quote_message(quote: Quote) -> str:
        payload = {
            "aid": "market_quote",
            "ok": True,
            "code": 0,
            "msg": "ok",
            "data": {
                "quote": quote.__dict__ if hasattr(quote, "__dict__") else quote,
                "symbol": quote.symbol,
                "exchange_id": quote.exchange_id,
                "instrument_id": quote.instrument_id,
                "trading_day": quote.trading_day,
                "update_time": quote.update_time,
            },
            "request_id": None,
            "conn_id": None,
        }
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))