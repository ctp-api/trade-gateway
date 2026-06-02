from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

from pyctp.gateway.ctp import CtpTraderAdapter, PybindTdApiAdapter
from pyctp.gateway.eventbus.bus import Event, EventBus
from pyctp.gateway.protocol import ProtocolCodec
from pyctp.gateway.protocol.types import (
    CancelOrderRequest,
    InsertOrderRequest,
    LoginRequest,
    WsRequest,
    WsResponse,
)
from pyctp.gateway.trader.notify import TraderNotify, TraderNotifyType
from pyctp.gateway.trader.persistence import TraderPersistence, TraderPersistenceData
from pyctp.gateway.websocket import WebSocketServer


class TraderState(str, Enum):
    INIT = "init"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    AUTHENTICATING = "authenticating"
    AUTHENTICATED = "authenticated"
    LOGGING_IN = "logging_in"
    LOGGED_IN = "logged_in"
    SETTLEMENT_QUERYING = "settlement_querying"
    CONFIRMING_SETTLEMENT = "confirming_settlement"
    READY = "ready"
    STOPPING = "stopping"
    STOPPED = "stopped"


TRADER_STATE_TRANSITIONS: dict[TraderState, set[TraderState]] = {
    TraderState.INIT: {TraderState.CONNECTING, TraderState.STOPPING},
    TraderState.CONNECTING: {TraderState.CONNECTED, TraderState.READY, TraderState.STOPPING, TraderState.INIT},
    TraderState.CONNECTED: {TraderState.AUTHENTICATING, TraderState.READY, TraderState.STOPPING},
    TraderState.AUTHENTICATING: {TraderState.AUTHENTICATED, TraderState.READY, TraderState.STOPPING},
    TraderState.AUTHENTICATED: {TraderState.LOGGING_IN, TraderState.READY, TraderState.STOPPING},
    TraderState.LOGGING_IN: {TraderState.LOGGED_IN, TraderState.READY, TraderState.STOPPING},
    TraderState.LOGGED_IN: {TraderState.SETTLEMENT_QUERYING, TraderState.READY, TraderState.STOPPING},
    TraderState.SETTLEMENT_QUERYING: {TraderState.CONFIRMING_SETTLEMENT, TraderState.READY, TraderState.STOPPING},
    TraderState.CONFIRMING_SETTLEMENT: {TraderState.READY, TraderState.STOPPING},
    TraderState.READY: {TraderState.CONNECTING, TraderState.STOPPING},
    TraderState.STOPPING: {TraderState.STOPPED},
    TraderState.STOPPED: {TraderState.INIT},
}


@dataclass(slots=True)
class TraderConfig:
    host: str = "0.0.0.0"
    port: int = 7788
    log_level: str = "INFO"
    data_dir: Path = field(default_factory=lambda: Path("./data"))
    ctp_appid: str = "simnow_client_test"
    ctp_auth_code: str = "0000000000000000"


class TraderEngine:
    def __init__(self, bus: EventBus, config: TraderConfig, ctp: CtpTraderAdapter | None = None) -> None:
        self.bus = bus
        self.config = config
        self.ctp = ctp or CtpTraderAdapter(PybindTdApiAdapter(bus=bus))
        self.codec = ProtocolCodec()
        self.ws = WebSocketServer(config.host, config.port, bus)
        self.state = TraderState.INIT
        self._tasks: list[asyncio.Task[Any]] = []
        self._stop_event = asyncio.Event()
        self._started = False
        self._pending_login_conn_id: int | None = None
        self._login_request: LoginRequest | None = None
        self._order_conn_map: dict[str, int] = {}
        self._request_seq = 0
        self._query_pending: dict[str, dict[str, Any]] = {}
        self._query_results: dict[str, list[dict[str, Any]]] = {}
        self._persistence = TraderPersistence(config.data_dir)
        self._last_trading_day: str = ""
        self._last_account_snapshot: dict[str, Any] = {}
        self._last_position_snapshot: dict[str, int] = {}
        self._last_trade_snapshot: dict[str, dict[str, Any]] = {}
        self._query_results: dict[str, list[dict[str, Any]]] = {
            "account": [],
            "position": [],
            "order": [],
            "trade": [],
            "instrument": [],
        }
        self._system_notify_queue: list[dict[str, Any]] = []

    def _next_request_id(self) -> int:
        self._request_seq += 1
        return self._request_seq

    def _set_state(self, state: TraderState) -> None:
        previous = self.state
        allowed = TRADER_STATE_TRANSITIONS.get(previous, set())
        if previous != state and allowed and state not in allowed:
            if state == TraderState.INIT and previous == TraderState.STOPPED:
                self.state = state
                self._queue_system_notify(f"state changed: {previous.value} -> {state.value}", msg_type=TraderNotifyType.STATE, data={"from": previous.value, "to": state.value})
                return
            self._notify_error("invalid state transition", category=TraderNotifyType.ERROR_SYSTEM, data={"from": previous.value, "to": state.value})
            return
        self.state = state
        if previous != state and previous != TraderState.INIT:
            self._queue_system_notify(f"state changed: {previous.value} -> {state.value}", msg_type=TraderNotifyType.STATE, data={"from": previous.value, "to": state.value})
            if previous == TraderState.READY and state == TraderState.CONNECTING:
                current_day = self._get_trading_day()
                self._notify_trading_day_change(self._last_trading_day, current_day, meta={"reason": "reconnect"})
                self._last_trading_day = current_day

    @staticmethod
    def _notify_type_value(kind: TraderNotifyType | str) -> str:
        return kind.value if isinstance(kind, TraderNotifyType) else str(kind)

    def _notify_error(self, msg: str, code: int = 500, category: TraderNotifyType | str = TraderNotifyType.ERROR_SYSTEM, data: dict[str, Any] | None = None) -> None:
        self._queue_system_notify(msg, code=code, level="ERROR", msg_type=self._notify_type_value(category), data=data)

    def _notify_query_complete(self, kind: str, count: int, ok: bool = True, error: str = "") -> None:
        payload = {"kind": kind, "count": count, "ok": ok}
        if error:
            payload["error"] = error
        self._queue_system_notify(f"query complete: {kind} count={count}", level="INFO" if ok else "ERROR", msg_type=self._notify_type_value(TraderNotifyType.QUERY), data=payload)

    def _notify_account_change(self, kind: str, before: float, after: float, meta: dict[str, Any] | None = None) -> None:
        payload = {"kind": kind, "before": before, "after": after, "delta": after - before}
        if meta:
            payload.update(meta)
        self._queue_system_notify(f"account change: {kind} {before} -> {after}", msg_type=self._notify_type_value(TraderNotifyType.ACCOUNT), data=payload)

    def _notify_position_change(self, instrument_id: str, before: int, after: int, meta: dict[str, Any] | None = None) -> None:
        payload = {"instrument_id": instrument_id, "before": before, "after": after, "delta": after - before}
        if meta:
            payload.update(meta)
        self._queue_system_notify(f"position change: {instrument_id} {before} -> {after}", msg_type=self._notify_type_value(TraderNotifyType.POSITION), data=payload)

    def _notify_order_status_change(self, order_ref: str, before: str, after: str, meta: dict[str, Any] | None = None) -> None:
        payload = {"order_ref": order_ref, "before": before, "after": after}
        if meta:
            payload.update(meta)
        self._queue_system_notify(f"order status change: {order_ref} {before} -> {after}", msg_type=self._notify_type_value(TraderNotifyType.ORDER), data=payload)

    def _notify_trading_day_change(self, before: str, after: str, meta: dict[str, Any] | None = None) -> None:
        payload = {"before": before, "after": after}
        if meta:
            payload.update(meta)
        self._queue_system_notify(f"trading day changed: {before} -> {after}", msg_type=self._notify_type_value(TraderNotifyType.TRADING_DAY), data=payload)

    def is_ready(self) -> bool:
        return self.state == TraderState.READY

    def is_running(self) -> bool:
        return self.state not in {TraderState.STOPPING, TraderState.STOPPED}

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        self.bus.bind_loop(asyncio.get_running_loop())
        await self.ws.start()
        self._set_state(TraderState.READY)
        self._tasks.append(asyncio.create_task(self._event_loop(), name="trader-event-loop"))
        self._tasks.append(asyncio.create_task(self._idle_loop(), name="trader-idle-loop"))
        self._tasks.append(asyncio.create_task(self._flush_persistence_loop(), name="trader-persistence-loop"))

    async def run_forever(self) -> None:
        await self._stop_event.wait()

    async def stop(self) -> None:
        if self.state in {TraderState.STOPPING, TraderState.STOPPED}:
            return
        self._set_state(TraderState.STOPPING)
        self._stop_event.set()
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        self._pending_login_conn_id = None
        self._login_request = None
        self._query_pending.clear()
        self._order_conn_map.clear()
        await self.ws.stop()
        self._set_state(TraderState.STOPPED)

    async def _event_loop(self) -> None:
        while self.state != TraderState.STOPPED:
            try:
                event = await self.bus.get()
                await self.handle_event(event)
            except asyncio.CancelledError:
                break

    async def _idle_loop(self) -> None:
        while self.state != TraderState.STOPPED:
            try:
                await asyncio.sleep(0.1)
                await self.handle_event(Event(type="timer.idle", source="engine"))
            except asyncio.CancelledError:
                break

    async def _flush_persistence_loop(self) -> None:
        while self.state != TraderState.STOPPED:
            try:
                await asyncio.sleep(1.0)
                self._save_persistence()
                await self._drain_system_notify_queue()
            except asyncio.CancelledError:
                break
            except Exception:
                continue

    async def handle_event(self, event: Event) -> None:
        if event.type == "ws.connected":
            conn_id = event.conn_id or 0
            self._set_state(TraderState.CONNECTED)
            await self._send_notify(conn_id, f"connected: {conn_id}")
            return

        if event.type == "ws.message":
            conn_id = event.conn_id or 0
            raw = str(event.payload.get("message", ""))
            try:
                req = self.codec.parse_request(raw, conn_id=conn_id)
            except Exception as exc:
                await self.ws.send_to(conn_id, self.codec.build_response(WsResponse(aid="error", ok=False, code=400, msg=str(exc), conn_id=conn_id, request_id=self._next_request_id())))
                return

            req.request_id = self._next_request_id()
            resp = await self.dispatch_request(req)
            if resp.request_id is None:
                resp.request_id = req.request_id
            await self.ws.send_to(conn_id, self.codec.build_response(resp))
            return

        if event.type == "ctp.front_disconnected":
            await self._on_front_disconnected(event)
            return

        if event.type == "ctp.rsp_authenticate":
            await self._on_rsp_authenticate(event)
            return

        if event.type == "ctp.rsp_user_login":
            await self._on_rsp_user_login(event)
            return

        if event.type == "ctp.rsp_settlement_info_confirm":
            await self._on_rsp_settlement_info_confirm(event)
            return

        if event.type == "timer.idle":
            self._save_persistence()
            await self._drain_system_notify_queue()
            return

        if event.type == "ctp.rsp_qry_trading_account":
            await self._on_query_result(event, "account")
            return

        if event.type == "ctp.rsp_qry_investor_position":
            await self._on_query_result(event, "position")
            return

        if event.type == "ctp.rsp_qry_order":
            await self._on_query_result(event, "order")
            return

        if event.type == "ctp.rsp_qry_trade":
            await self._on_query_result(event, "trade")
            return

        if event.type == "ctp.rsp_qry_instrument":
            await self._on_query_result(event, "instrument")
            return

        if event.type == "ctp.rsp_qry_trading_account_error":
            self._notify_error("query trading account failed", category="QUERY", data=event.payload)
            return

        if event.type == "ctp.rsp_qry_investor_position_error":
            self._notify_error("query investor position failed", category="QUERY", data=event.payload)
            return

        if event.type == "ctp.rsp_qry_order_error":
            self._notify_error("query order failed", category="QUERY", data=event.payload)
            return

        if event.type == "ctp.rsp_qry_trade_error":
            self._notify_error("query trade failed", category="QUERY", data=event.payload)
            return

        if event.type == "ctp.rsp_qry_instrument_error":
            self._notify_error("query instrument failed", category="QUERY", data=event.payload)
            return

        if event.type == "ctp.rsp_qry_trading_account_error":
            self._notify_error("query trading account failed", category="QUERY", data=event.payload)
            return

        if event.type == "ctp.rsp_qry_investor_position_error":
            self._notify_error("query investor position failed", category="QUERY", data=event.payload)
            return

        if event.type == "ctp.rsp_qry_order_error":
            self._notify_error("query order failed", category="QUERY", data=event.payload)
            return

        if event.type == "ctp.rsp_qry_trade_error":
            self._notify_error("query trade failed", category="QUERY", data=event.payload)
            return

        if event.type == "ctp.rsp_qry_instrument_error":
            self._notify_error("query instrument failed", category="QUERY", data=event.payload)
            return

        if event.type == "ctp.rsp_settlement_info_confirm":
            await self._broadcast_notify("settlement info confirmed", msg_type="SETTLEMENT")
            return

        if event.type == "ctp.rsp_order_insert":
            await self._on_rsp_order_insert(event)
            return

        if event.type == "ctp.err_order_insert":
            await self._on_err_rtn_order_insert(event)
            return

        if event.type == "ctp.rsp_order_action":
            await self._on_rsp_order_action(event)
            return

        if event.type == "ctp.rtn_order":
            await self._on_rtn_order(event)
            return

        if event.type == "ctp.rtn_trade":
            await self._on_rtn_trade(event)
            return

    def _extract_payload(self, req: WsRequest) -> dict[str, Any]:
        payload = req.raw.get("data")
        if not isinstance(payload, dict):
            raise ValueError("missing data payload")
        return payload

    def _mk_response(self, aid: str, req: WsRequest, ok: bool, code: int, msg: str, data: dict[str, Any] | None = None, request_id: int | None = None) -> WsResponse:
        return WsResponse(aid=aid, ok=ok, code=code, msg=msg, data=data or {}, conn_id=req.conn_id, request_id=request_id if request_id is not None else req.request_id)

    def _build_notify_payload(self, msg: str, code: int = 0, level: str = "INFO", msg_type: TraderNotifyType | str = TraderNotifyType.NOTIFY, data: dict[str, Any] | None = None) -> dict[str, Any]:
        return TraderNotify.build_payload(msg, msg_type, code=code, level=level, data=data)

    def _build_notify(self, msg: str, code: int = 0, level: str = "INFO", msg_type: TraderNotifyType | str = TraderNotifyType.NOTIFY, data: dict[str, Any] | None = None) -> str:
        return self.codec.dumps(self._build_notify_payload(msg=msg, code=code, level=level, msg_type=msg_type, data=data))

    def _queue_system_notify(self, msg: str, code: int = 0, level: str = "INFO", msg_type: TraderNotifyType | str = TraderNotifyType.NOTIFY, data: dict[str, Any] | None = None) -> None:
        self._system_notify_queue.append(self._build_notify_payload(msg=msg, code=code, level=level, msg_type=msg_type, data=data))

    async def _drain_system_notify_queue(self) -> None:
        while self._system_notify_queue:
            item = self._system_notify_queue.pop(0)
            await self._broadcast_notify_payload(item)

    async def _broadcast_notify_payload(self, payload: dict[str, Any]) -> None:
        await self.ws.broadcast(self.codec.dumps(payload))

    async def _send_notify_payload(self, conn_id: int, payload: dict[str, Any]) -> None:
        if conn_id:
            await self.ws.send_to(conn_id, self.codec.dumps(payload))

    async def _notify_query_complete_event(self, kind: str, rows: list[dict[str, Any]], ok: bool = True, error: str = "") -> None:
        self._notify_query_complete(kind, len(rows), ok=ok, error=error)
        await self._drain_system_notify_queue()

    async def _notify_query_complete_event(self, kind: str, rows: list[dict[str, Any]], ok: bool = True, error: str = "") -> None:
        self._notify_query_complete(kind, len(rows), ok=ok, error=error)
        await self._drain_system_notify_queue()

    async def _notify_balance_change_event(self, before: float, after: float, meta: dict[str, Any] | None = None) -> None:
        self._notify_account_change("balance", before, after, meta=meta)
        await self._drain_system_notify_queue()

    async def _notify_position_change_event(self, instrument_id: str, before: int, after: int, meta: dict[str, Any] | None = None) -> None:
        self._notify_position_change(instrument_id, before, after, meta=meta)
        await self._drain_system_notify_queue()

    def _notify_trade_summary(self, order_ref: str, instrument_id: str, total_volume: int, total_amount: float, trade_count: int, meta: dict[str, Any] | None = None) -> None:
        payload = {
            "order_ref": order_ref,
            "instrument_id": instrument_id,
            "total_volume": total_volume,
            "total_amount": total_amount,
            "trade_count": trade_count,
        }
        if meta:
            payload.update(meta)
        self._queue_system_notify(
            f"trade summary: {order_ref or instrument_id} volume={total_volume} trades={trade_count}",
            msg_type=self._notify_type_value(TraderNotifyType.TRADE_SUMMARY),
            data=payload,
        )

    async def _send_notify(self, conn_id: int, msg: str, code: int = 0, level: str = "INFO", msg_type: str = "NOTIFY", data: dict[str, Any] | None = None) -> None:
        if conn_id:
            await self._send_notify_payload(conn_id, self._build_notify_payload(msg=msg, code=code, level=level, msg_type=msg_type, data=data))

    async def _broadcast_notify(self, msg: str, code: int = 0, level: str = "INFO", msg_type: str = "NOTIFY", data: dict[str, Any] | None = None) -> None:
        await self._broadcast_notify_payload(self._build_notify_payload(msg=msg, code=code, level=level, msg_type=msg_type, data=data))

    async def dispatch_request(self, req: WsRequest) -> WsResponse:
        if req.aid == "req_login":
            login = self.codec.parse_login(req)
            login.appid = login.appid or self.config.ctp_appid
            login.auth_code = login.auth_code or self.config.ctp_auth_code
            return await self.handle_req_login(req, login)
        if req.aid == "insert_order":
            order = self.codec.parse_insert_order(req)
            return await self.handle_insert_order(req, order)
        if req.aid == "cancel_order":
            cancel = self.codec.parse_cancel_order(req)
            return await self.handle_cancel_order(req, cancel)
        if req.aid == "query_trading_account":
            return await self.handle_query_trading_account(req)
        if req.aid == "query_investor_position":
            return await self.handle_query_investor_position(req)
        if req.aid == "query_order":
            return await self.handle_query_order(req)
        if req.aid == "query_trade":
            return await self.handle_query_trade(req)
        if req.aid == "query_instrument":
            return await self.handle_query_instrument(req)

        handler = getattr(self, f"handle_{req.aid}", None)
        if handler is None:
            return self._mk_response(req.aid, req, False, 404, f"unsupported aid: {req.aid}")
        result = handler(req)
        if asyncio.iscoroutine(result):
            result = await result
        if isinstance(result, WsResponse):
            result.request_id = result.request_id or self._next_request_id()
            return result
        return self._mk_response(req.aid, req, True, 0, "ok", result or {})

    async def handle_req_login(self, req: WsRequest, login: LoginRequest) -> WsResponse:
        if not login.user_name or not login.password:
            return self._mk_response(req.aid, req, False, 400, "missing user_name or password")
        if not login.broker_id or not login.front:
            return self._mk_response(req.aid, req, False, 400, "missing broker_id or front")

        self._pending_login_conn_id = req.conn_id
        self._login_request = login
        self._set_state(TraderState.CONNECTING)

        try:
            self.ctp.connect(login.front, login.user_name, login.password, login.broker_id, login.auth_code, login.appid)
            self._set_state(TraderState.AUTHENTICATING)
        except Exception as exc:
            self._set_state(TraderState.READY)
            self._pending_login_conn_id = None
            self._login_request = None
            return self._mk_response(req.aid, req, False, 500, f"ctp connect failed: {exc}")

        return self._mk_response(req.aid, req, True, 0, "login request accepted", {"user_name": login.user_name, "broker_id": login.broker_id, "front": login.front, "appid": login.appid, "auth_code": login.auth_code, "status": self.state.value})

    def _can_accept_query(self) -> bool:
        return self.state in {TraderState.READY, TraderState.LOGGED_IN, TraderState.SETTLEMENT_QUERYING, TraderState.CONFIRMING_SETTLEMENT}

    def _can_accept_order(self) -> bool:
        return self.state in {TraderState.READY, TraderState.LOGGED_IN}

    async def handle_query_trading_account(self, req: WsRequest) -> WsResponse:
        if not self._can_accept_query():
            return self._mk_response(req.aid, req, False, 409, f"trader not ready for query: {self.state.value}")
        try:
            self.ctp.clear_query_rows("account")
            request_id = self._next_request_id()
            self._query_pending["account"] = {"conn_id": req.conn_id, "request_id": request_id, "rows": []}
            self.ctp.query_trading_account()
        except Exception as exc:
            self._query_pending.pop("account", None)
            return self._mk_response(req.aid, req, False, 500, f"ctp query trading account failed: {exc}", request_id=request_id if 'request_id' in locals() else None)
        return self._mk_response(req.aid, req, True, 0, "query trading account accepted", {"status": "pending"}, request_id=request_id)

    async def handle_query_investor_position(self, req: WsRequest) -> WsResponse:
        if not self._can_accept_query():
            return self._mk_response(req.aid, req, False, 409, f"trader not ready for query: {self.state.value}")
        try:
            self.ctp.clear_query_rows("position")
            request_id = self._next_request_id()
            self._query_pending["position"] = {"conn_id": req.conn_id, "request_id": request_id, "rows": []}
            self.ctp.query_investor_position()
        except Exception as exc:
            self._query_pending.pop("position", None)
            return self._mk_response(req.aid, req, False, 500, f"ctp query investor position failed: {exc}", request_id=request_id if 'request_id' in locals() else None)
        return self._mk_response(req.aid, req, True, 0, "query investor position accepted", {"status": "pending"}, request_id=request_id)

    async def handle_query_order(self, req: WsRequest) -> WsResponse:
        if not self._can_accept_query():
            return self._mk_response(req.aid, req, False, 409, f"trader not ready for query: {self.state.value}")
        try:
            self.ctp.clear_query_rows("order")
            request_id = self._next_request_id()
            self._query_pending["order"] = {"conn_id": req.conn_id, "request_id": request_id, "rows": []}
            self.ctp.query_order()
        except Exception as exc:
            self._query_pending.pop("order", None)
            return self._mk_response(req.aid, req, False, 500, f"ctp query order failed: {exc}", request_id=request_id if 'request_id' in locals() else None)
        return self._mk_response(req.aid, req, True, 0, "query order accepted", {"status": "pending"}, request_id=request_id)

    async def handle_query_trade(self, req: WsRequest) -> WsResponse:
        if not self._can_accept_query():
            return self._mk_response(req.aid, req, False, 409, f"trader not ready for query: {self.state.value}")
        try:
            self.ctp.clear_query_rows("trade")
            request_id = self._next_request_id()
            self._query_pending["trade"] = {"conn_id": req.conn_id, "request_id": request_id, "rows": []}
            self.ctp.query_trade()
        except Exception as exc:
            self._query_pending.pop("trade", None)
            return self._mk_response(req.aid, req, False, 500, f"ctp query trade failed: {exc}", request_id=request_id if 'request_id' in locals() else None)
        return self._mk_response(req.aid, req, True, 0, "query trade accepted", {"status": "pending"}, request_id=request_id)

    async def handle_query_instrument(self, req: WsRequest) -> WsResponse:
        if not self._can_accept_query():
            return self._mk_response(req.aid, req, False, 409, f"trader not ready for query: {self.state.value}")
        payload = self._extract_payload(req)
        instrument_id = str(payload.get("instrument_id", ""))
        try:
            self.ctp.clear_query_rows("instrument")
            request_id = self._next_request_id()
            self._query_pending["instrument"] = {"conn_id": req.conn_id, "request_id": request_id, "rows": [], "instrument_id": instrument_id}
            self.ctp.query_instrument(instrument_id)
        except Exception as exc:
            self._query_pending.pop("instrument", None)
            return self._mk_response(req.aid, req, False, 500, f"ctp query instrument failed: {exc}", request_id=request_id if 'request_id' in locals() else None)
        return self._mk_response(req.aid, req, True, 0, "query instrument accepted", {"status": "pending", "instrument_id": instrument_id}, request_id=request_id)

    async def _on_front_disconnected(self, event: Event) -> None:
        self._pending_login_conn_id = None
        self._login_request = None
        self._query_pending.clear()
        self._queue_system_notify("front disconnected", msg_type=TraderNotifyType.ERROR_SYSTEM, level="ERROR", data={"reason": event.payload.get("reason", 0)})
        if self.state not in {TraderState.STOPPING, TraderState.STOPPED}:
            self._set_state(TraderState.READY)
            self._set_state(TraderState.INIT)

    async def _on_rsp_authenticate(self, event: Event) -> None:
        payload = event.payload
        error = payload.get("error", {}) or {}
        data = payload.get("data", {}) or {}
        conn_id = self._pending_login_conn_id or 0
        if int(error.get("ErrorID", 0)) != 0:
            self._set_state(TraderState.READY)
            self._pending_login_conn_id = None
            self._login_request = None
            request_id = int((self._query_pending.get("account", {}) or {}).get("request_id") or self._next_request_id())
            await self.ws.send_to(conn_id, self.codec.build_response(WsResponse(aid="req_login", ok=False, code=500, msg=f"authenticate failed: {error.get('ErrorMsg', 'unknown')}", data={"data": data, "error": error}, conn_id=conn_id, request_id=request_id)))
            return
        self._set_state(TraderState.AUTHENTICATED)
        self._set_state(TraderState.LOGGING_IN)
        self.ctp.login()

    async def _on_rsp_user_login(self, event: Event) -> None:
        payload = event.payload
        error = payload.get("error", {}) or {}
        data = payload.get("data", {}) or {}
        conn_id = self._pending_login_conn_id or 0
        if int(error.get("ErrorID", 0)) != 0:
            self._set_state(TraderState.READY)
            self._pending_login_conn_id = None
            self._login_request = None
            self._notify_error("login failed", code=int(error.get("ErrorID", 500) or 500), category=TraderNotifyType.ERROR_SYSTEM, data={"data": data, "error": error})
            await self.ws.send_to(conn_id, self.codec.build_response(WsResponse(aid="req_login", ok=False, code=500, msg=f"login failed: {error.get('ErrorMsg', 'unknown')}", data={"data": data, "error": error}, conn_id=conn_id, request_id=event.request_id)))
            return
        self._set_state(TraderState.LOGGED_IN)
        self._queue_system_notify("user logged in", msg_type=TraderNotifyType.STATE, data={"stage": "logged_in"})
        self._set_state(TraderState.SETTLEMENT_QUERYING)
        self._queue_system_notify("settlement querying started", msg_type=TraderNotifyType.SETTLEMENT, data={"stage": "querying"})
        self._set_state(TraderState.CONFIRMING_SETTLEMENT)
        self._queue_system_notify("settlement confirming started", msg_type=TraderNotifyType.SETTLEMENT, data={"stage": "confirming"})
        await self._send_notify(conn_id, "login success, confirming settlement...")

    async def _on_rsp_settlement_info_confirm(self, event: Event) -> None:
        payload = event.payload
        error = payload.get("error", {}) or {}
        data = payload.get("data", {}) or {}
        conn_id = self._pending_login_conn_id or 0
        if int(error.get("ErrorID", 0)) != 0:
            self._set_state(TraderState.READY)
            self._pending_login_conn_id = None
            self._login_request = None
            self._notify_error("settlement confirm failed", code=int(error.get("ErrorID", 500) or 500), category=TraderNotifyType.ERROR_SETTLEMENT, data={"data": data, "error": error})
            await self._send_notify(conn_id, f"settlement confirm failed: {error.get('ErrorMsg', 'unknown')}", code=int(error.get("ErrorID", 500) or 500), level="ERROR", msg_type=TraderNotifyType.SETTLEMENT, data={"data": data, "error": error})
            await self.ws.send_to(conn_id, self.codec.build_response(WsResponse(aid="req_login", ok=False, code=500, msg=f"settlement confirm failed: {error.get('ErrorMsg', 'unknown')}", data={"data": data, "error": error}, conn_id=conn_id, request_id=event.request_id)))
            return
        self._set_state(TraderState.READY)
        self._queue_system_notify("settlement confirmed", msg_type=TraderNotifyType.SETTLEMENT, data={"status": "confirmed"})
        if conn_id:
            await self._send_notify(conn_id, "login success", msg_type=TraderNotifyType.SETTLEMENT, data={"status": "ready"})
            await self.ws.send_to(conn_id, self.codec.build_response(WsResponse(aid="req_login", ok=True, code=0, msg="login success", data={"data": data, "error": error, "status": "ready"}, conn_id=conn_id, request_id=event.request_id)))
        self._pending_login_conn_id = None
        self._login_request = None
        self._save_persistence()

    async def _on_query_result(self, event: Event, kind: str) -> None:
        payload = event.payload
        data = payload.get("data", {}) or {}
        error = payload.get("error", {}) or {}
        last = bool(payload.get("last", False))
        pending = self._query_pending.setdefault(kind, {})
        if int(error.get("ErrorID", 0)) == 0:
            pending.setdefault("rows", []).append(data)
        if not last:
            return
        conn_id = int(pending.get("conn_id") or self._pending_login_conn_id or 0)
        rows = self._normalize_query_rows(kind, list(pending.get("rows", [])))
        previous_rows = list(self._query_results.get(kind, []))
        self._query_results[kind] = rows
        response_kind = {
            "account": "query_account",
            "position": "query_position",
            "order": "query_order",
            "trade": "query_trade",
            "instrument": "query_instrument",
        }.get(kind, f"query_{kind}")
        response = WsResponse(
            aid=response_kind,
            ok=int(error.get("ErrorID", 0)) == 0,
            code=0 if int(error.get("ErrorID", 0)) == 0 else int(error.get("ErrorID", 500) or 500),
            msg=error.get("ErrorMsg", "ok" if int(error.get("ErrorID", 0)) == 0 else "unknown"),
            data={"rows": rows, "count": len(rows)},
            conn_id=conn_id,
            request_id=int(pending.get("request_id") or event.request_id or self._next_request_id()),
        )
        if conn_id:
            await self.ws.send_to(conn_id, self.codec.build_response(response))
        await self._notify_query_complete_event(kind, rows, ok=int(error.get("ErrorID", 0)) == 0, error=str(error.get("ErrorMsg", "")))
        if kind == "account" and previous_rows and rows:
            before = float(previous_rows[0].get("balance", 0.0) or 0.0)
            after = float(rows[0].get("balance", 0.0) or 0.0)
            if before != after:
                await self._notify_balance_change_event(before, after, meta={"kind": "account.balance"})
            self._last_account_snapshot = {"balance": after, "available": float(rows[0].get("available", 0.0) or 0.0), "trading_day": str(rows[0].get("trading_day", ""))}
        elif kind == "account" and rows:
            self._last_account_snapshot = {"balance": float(rows[0].get("balance", 0.0) or 0.0), "available": float(rows[0].get("available", 0.0) or 0.0), "trading_day": str(rows[0].get("trading_day", ""))}
        if kind == "position" and previous_rows and rows:
            prev_map = {str(item.get("instrument_id", "")): int(item.get("position", 0) or 0) for item in previous_rows}
            current_map: dict[str, int] = {}
            for item in rows:
                instrument_id = str(item.get("instrument_id", ""))
                before = int(prev_map.get(instrument_id, 0))
                after = int(item.get("position", 0) or 0)
                current_map[instrument_id] = after
                if before != after:
                    await self._notify_position_change_event(instrument_id, before, after, meta={"kind": "position.update"})
            self._last_position_snapshot = current_map
        elif kind == "position" and rows:
            self._last_position_snapshot = {str(item.get("instrument_id", "")): int(item.get("position", 0) or 0) for item in rows}
        if kind == "trade" and rows:
            total_volume = 0
            total_amount = 0.0
            trade_count = len(rows)
            order_ref = ""
            instrument_id = ""
            for item in rows:
                total_volume += int(item.get("volume", 0) or 0)
                total_amount += float(item.get("price", 0.0) or 0.0) * int(item.get("volume", 0) or 0)
                order_ref = order_ref or str(item.get("order_ref", ""))
                instrument_id = instrument_id or str(item.get("instrument_id", ""))
            key = order_ref or instrument_id
            previous_summary = self._last_trade_snapshot.get(key, {})
            if previous_summary.get("total_volume") != total_volume or previous_summary.get("trade_count") != trade_count:
                self._notify_trade_summary(order_ref, instrument_id, total_volume, total_amount, trade_count, meta={"kind": "trade.query"})
            self._last_trade_snapshot[key] = {"total_volume": total_volume, "total_amount": total_amount, "trade_count": trade_count}
        if kind == "account" and rows:
            current_day = str(rows[0].get("trading_day", ""))
            if self._last_trading_day and current_day and self._last_trading_day != current_day:
                self._notify_trading_day_change(self._last_trading_day, current_day, meta={"kind": "account.query"})
            self._last_trading_day = current_day or self._last_trading_day
        self._query_pending.pop(kind, None)

    async def _on_rsp_order_insert(self, event: Event) -> None:
        payload = event.payload
        data = payload.get("data", {}) or {}
        error = payload.get("error", {}) or {}
        order_ref = str(data.get("OrderRef", "") or data.get("order_ref", "") or "")
        conn_id = self._order_conn_map.get(order_ref, self._pending_login_conn_id or 0)
        before_status = str(self.ctp.order_status_map.get(order_ref, "")) if hasattr(self.ctp, "order_status_map") else ""
        if int(error.get("ErrorID", 0)) != 0:
            await self._send_order_error(conn_id, "rsp_order_insert", order_ref, data, error)
            self._notify_error("order insert rejected", category="ORDER", data={"order_ref": order_ref, "data": data, "error": error})
            return
        after_status = str(data.get("OrderStatus", "insert_submitted") or "insert_submitted")
        if order_ref and before_status != after_status:
            self._notify_order_status_change(order_ref, before_status, after_status, meta={"event": "rsp_order_insert"})
        await self._push_order_event(conn_id, "rsp_order_insert", data)

    async def _on_err_rtn_order_insert(self, event: Event) -> None:
        payload = event.payload
        data = payload.get("data", {}) or {}
        error = payload.get("error", {}) or {}
        order_ref = str(data.get("OrderRef", "") or data.get("order_ref", "") or "")
        conn_id = self._order_conn_map.get(order_ref, self._pending_login_conn_id or 0)
        await self._send_order_error(conn_id, "err_rtn_order_insert", order_ref, data, error)

    async def _on_rsp_order_action(self, event: Event) -> None:
        payload = event.payload
        data = payload.get("data", {}) or {}
        error = payload.get("error", {}) or {}
        order_ref = str(data.get("OrderRef", "") or data.get("order_ref", "") or "")
        conn_id = self._order_conn_map.get(order_ref, self._pending_login_conn_id or 0)
        before_status = str(self.ctp.order_status_map.get(order_ref, "")) if hasattr(self.ctp, "order_status_map") else ""
        if int(error.get("ErrorID", 0)) != 0:
            await self._send_order_error(conn_id, "rsp_order_action", order_ref, data, error)
            self._notify_error("order action rejected", category="ORDER", data={"order_ref": order_ref, "data": data, "error": error})
            return
        after_status = str(data.get("OrderStatus", "cancel_submitted") or "cancel_submitted")
        if order_ref and before_status != after_status:
            self._notify_order_status_change(order_ref, before_status, after_status, meta={"event": "rsp_order_action"})
        await self._push_order_event(conn_id, "rsp_order_action", data)

    async def _on_rtn_order(self, event: Event) -> None:
        data = event.payload.get("data", {}) or {}
        order_ref = str(data.get("OrderRef", ""))
        conn_id = self._order_conn_map.get(order_ref, self._pending_login_conn_id or 0)
        before_status = str(self.ctp.order_status_map.get(order_ref, "")) if hasattr(self.ctp, "order_status_map") else ""
        after_status = str(data.get("OrderStatus", "") or "")
        if order_ref and before_status != after_status:
            self._notify_order_status_change(order_ref, before_status, after_status, meta={"event": "rtn_order"})
        await self.ws.broadcast(self.codec.dumps({"aid": "rtn_order", "ok": True, "data": data}))
        if conn_id:
            await self._send_notify(conn_id, "rtn_order received", msg_type="ORDER")

    async def _on_rtn_trade(self, event: Event) -> None:
        data = event.payload.get("data", {}) or {}
        order_ref = str(data.get("OrderRef", ""))
        conn_id = self._order_conn_map.get(order_ref, self._pending_login_conn_id or 0)
        if order_ref:
            self._notify_order_status_change(order_ref, "", "part_traded", meta={"event": "rtn_trade"})
        await self.ws.broadcast(self.codec.dumps({"aid": "rtn_trade", "ok": True, "data": data}))
        if conn_id:
            await self._send_notify(conn_id, "rtn_trade received", msg_type="TRADE")

    async def _push_order_event(self, conn_id: int, aid: str, data: dict[str, Any]) -> None:
        if conn_id:
            await self.ws.send_to(conn_id, self.codec.dumps({"aid": aid, "ok": True, "data": data}))

    async def _send_order_error(self, conn_id: int, aid: str, order_ref: str, data: dict[str, Any], error: dict[str, Any]) -> None:
        payload = {"aid": aid, "ok": False, "code": int(error.get("ErrorID", 500) or 500), "msg": error.get("ErrorMsg", "unknown"), "data": {"order_ref": order_ref, "data": data, "error": error}}
        if conn_id:
            await self.ws.send_to(conn_id, self.codec.dumps(payload))

    @staticmethod
    def _normalize_query_row(kind: str, row: dict[str, Any]) -> dict[str, Any]:
        if kind == "account":
            return {
                "account_id": str(row.get("AccountID", "")),
                "pre_balance": float(row.get("PreBalance", 0.0) or 0.0),
                "deposit": float(row.get("Deposit", 0.0) or 0.0),
                "withdraw": float(row.get("Withdraw", 0.0) or 0.0),
                "frozen_margin": float(row.get("FrozenMargin", row.get("CurrMargin", 0.0)) or 0.0),
                "frozen_cash": float(row.get("FrozenCash", 0.0) or 0.0),
                "frozen_commission": float(row.get("FrozenCommission", 0.0) or 0.0),
                "margin": float(row.get("CurrMargin", 0.0) or 0.0),
                "cash_in": float(row.get("CashIn", 0.0) or 0.0),
                "commission": float(row.get("Commission", 0.0) or 0.0),
                "close_profit": float(row.get("CloseProfit", 0.0) or 0.0),
                "position_profit": float(row.get("PositionProfit", 0.0) or 0.0),
                "balance": float(row.get("Balance", 0.0) or 0.0),
                "available": float(row.get("Available", 0.0) or 0.0),
                "withdraw_quota": float(row.get("WithdrawQuota", 0.0) or 0.0),
                "reserve_balance": float(row.get("ReserveBalance", 0.0) or 0.0),
                "currency_id": str(row.get("CurrencyID", "CNY")),
                "trading_day": str(row.get("TradingDay", "")),
                "settlement_id": int(row.get("SettlementID", 0) or 0),
                "raw": row,
            }
        if kind == "position":
            return {
                "instrument_id": str(row.get("InstrumentID", "")),
                "exchange_id": str(row.get("ExchangeID", "")),
                "posi_direction": str(row.get("PosiDirection", "")),
                "position": int(row.get("Position", 0) or 0),
                "yd_position": int(row.get("YdPosition", 0) or 0),
                "today_position": int(row.get("TodayPosition", 0) or 0),
                "long_frozen": int(row.get("LongFrozen", 0) or 0),
                "short_frozen": int(row.get("ShortFrozen", 0) or 0),
                "open_cost": float(row.get("OpenCost", 0.0) or 0.0),
                "position_cost": float(row.get("PositionCost", 0.0) or 0.0),
                "position_profit": float(row.get("PositionProfit", 0.0) or 0.0),
                "open_amount": float(row.get("OpenAmount", 0.0) or 0.0),
                "close_amount": float(row.get("CloseAmount", 0.0) or 0.0),
                "position_margin": float(row.get("PositionMargin", 0.0) or 0.0),
                "raw": row,
            }
        if kind == "order":
            return {
                "order_ref": str(row.get("OrderRef", "")),
                "instrument_id": str(row.get("InstrumentID", "")),
                "exchange_id": str(row.get("ExchangeID", "")),
                "direction": str(row.get("Direction", "")),
                "comb_offset_flag": str(row.get("CombOffsetFlag", "")),
                "comb_hedge_flag": str(row.get("CombHedgeFlag", "")),
                "limit_price": float(row.get("LimitPrice", 0.0) or 0.0),
                "volume_total_original": int(row.get("VolumeTotalOriginal", 0) or 0),
                "volume_traded": int(row.get("VolumeTraded", 0) or 0),
                "volume_total": int(row.get("VolumeTotal", 0) or 0),
                "time_condition": str(row.get("TimeCondition", "")),
                "volume_condition": str(row.get("VolumeCondition", "")),
                "contingent_condition": str(row.get("ContingentCondition", "")),
                "force_close_reason": str(row.get("ForceCloseReason", "")),
                "order_submit_status": str(row.get("OrderSubmitStatus", "")),
                "order_status": str(row.get("OrderStatus", "")),
                "order_sys_id": str(row.get("OrderSysID", "")),
                "order_local_id": str(row.get("OrderLocalID", "")).strip(),
                "front_id": int(row.get("FrontID", 0) or 0),
                "session_id": int(row.get("SessionID", 0) or 0),
                "status_msg": str(row.get("StatusMsg", "")),
                "insert_date": str(row.get("InsertDate", "")),
                "insert_time": str(row.get("InsertTime", "")),
                "update_time": str(row.get("UpdateTime", "")),
                "cancel_time": str(row.get("CancelTime", "")),
                "raw": row,
            }
        if kind == "trade":
            return {
                "trade_id": str(row.get("TradeID", "")),
                "order_ref": str(row.get("OrderRef", "")),
                "instrument_id": str(row.get("InstrumentID", "")),
                "exchange_id": str(row.get("ExchangeID", "")),
                "direction": str(row.get("Direction", "")),
                "offset_flag": str(row.get("OffsetFlag", "")),
                "hedge_flag": str(row.get("HedgeFlag", "")),
                "price": float(row.get("Price", 0.0) or 0.0),
                "volume": int(row.get("Volume", 0) or 0),
                "trade_date": str(row.get("TradeDate", "")),
                "trade_time": str(row.get("TradeTime", "")),
                "trade_type": str(row.get("TradeType", "")),
                "raw": row,
            }
        if kind == "instrument":
            return {
                "instrument_id": str(row.get("InstrumentID", "")),
                "exchange_id": str(row.get("ExchangeID", "")),
                "instrument_name": str(row.get("InstrumentName", "")),
                "product_class": str(row.get("ProductClass", "")),
                "volume_multiple": int(row.get("VolumeMultiple", 1) or 1),
                "price_tick": float(row.get("PriceTick", 0.0) or 0.0),
                "expire_date": str(row.get("ExpireDate", "")),
                "delivery_year": int(row.get("DeliveryYear", 0) or 0),
                "delivery_month": int(row.get("DeliveryMonth", 0) or 0),
                "raw": row,
            }
        return {"raw": row}

    @staticmethod
    def _normalize_query_rows(kind: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [TraderEngine._normalize_query_row(kind, row) for row in rows]

    def get_query_rows(self, kind: str) -> list[dict[str, Any]]:
        return list(self._query_results.get(kind, []))

    def get_account_rows(self) -> list[dict[str, Any]]:
        return self.get_query_rows("account")

    def get_position_rows(self) -> list[dict[str, Any]]:
        return self.get_query_rows("position")

    def get_order_rows(self) -> list[dict[str, Any]]:
        return self.get_query_rows("order")

    def get_trade_rows(self) -> list[dict[str, Any]]:
        return self.get_query_rows("trade")

    def get_instrument_rows(self) -> list[dict[str, Any]]:
        return self.get_query_rows("instrument")

    def _save_persistence(self) -> None:
        user_name = self._login_request.user_name if self._login_request else ""
        trading_day = self._get_trading_day()
        if not user_name:
            return
        data = TraderPersistenceData(
            user_name=user_name,
            trading_day=trading_day,
            order_conn_map=dict(self._order_conn_map),
            query_pending={k: dict(v) for k, v in self._query_pending.items()},
        )
        self._persistence.save(data)

    def _restore_persistence(self) -> None:
        user_name = self._login_request.user_name if self._login_request else ""
        if not user_name:
            return
        trading_day = self._get_trading_day()
        data = self._persistence.load(user_name, trading_day)
        self._order_conn_map.update(data.order_conn_map)
        self._query_pending.update(data.query_pending)

    def _get_trading_day(self) -> str:
        rows = self.get_account_rows()
        if rows:
            day = str(rows[0].get("trading_day", ""))
            if day:
                return day
        return ""

    def handle_ping(self, req: WsRequest) -> WsResponse:
        return WsResponse(aid="ping", ok=True, msg="pong", data={"pong": True}, conn_id=req.conn_id, request_id=req.request_id)

    def handle_echo(self, req: WsRequest) -> WsResponse:
        return WsResponse(aid="echo", ok=True, msg="ok", data={"raw": req.raw}, conn_id=req.conn_id, request_id=req.request_id)

    async def handle_insert_order(self, req: WsRequest, order: InsertOrderRequest) -> WsResponse:
        if not self._can_accept_order():
            return self._mk_response(req.aid, req, False, 409, f"trader not ready for order: {self.state.value}")
        symbol = self._build_symbol(order.exchange_id, order.instrument_id)
        direction = self._map_direction_from_request(order.direction, order.offset)
        if direction is None:
            return self._mk_response(req.aid, req, False, 400, f"unsupported direction/offset: {order.direction}/{order.offset}")
        try:
            order_ref = self.ctp.send_order(symbol, direction, order.price, order.volume)
        except Exception as exc:
            self._notify_error("send order failed", category=TraderNotifyType.ERROR_ORDER, data={"symbol": symbol, "direction": direction, "error": str(exc)})
            return self._mk_response(req.aid, req, False, 500, f"ctp send order failed: {exc}")
        if order_ref:
            self._order_conn_map[order_ref] = req.conn_id or 0
        return self._mk_response(req.aid, req, True, 0, "insert order accepted", {
            "order_ref": order_ref,
            "symbol": symbol,
            "direction": direction,
            "price": order.price,
            "volume": order.volume,
            "status": "accepted",
        })

    async def handle_cancel_order(self, req: WsRequest, cancel: CancelOrderRequest) -> WsResponse:
        if not self._can_accept_order():
            return self._mk_response(req.aid, req, False, 409, f"trader not ready for order: {self.state.value}")
        if not cancel.order_id:
            return self._mk_response(req.aid, req, False, 400, "missing order_id")
        try:
            self.ctp.cancel_order(cancel.order_id, cancel.exchange_id, cancel.instrument_id)
        except Exception as exc:
            self._notify_error("cancel order failed", category=TraderNotifyType.ERROR_ORDER, data={"order_id": cancel.order_id, "error": str(exc)})
            return self._mk_response(req.aid, req, False, 500, f"ctp cancel order failed: {exc}")
        if req.conn_id is not None:
            self._order_conn_map.setdefault(cancel.order_id, req.conn_id)
        return self._mk_response(req.aid, req, True, 0, "cancel order accepted", {
            "order_id": cancel.order_id,
            "exchange_id": cancel.exchange_id,
            "instrument_id": cancel.instrument_id,
            "status": "accepted",
        })

    @staticmethod
    def _build_symbol(exchange_id: str, instrument_id: str) -> str:
        exchange_id = exchange_id.strip().upper()
        instrument_id = instrument_id.strip()
        if not exchange_id:
            return instrument_id
        if exchange_id == "SHFE":
            instrument_id = instrument_id.lower()
        return f"{exchange_id}.{instrument_id}"

    @staticmethod
    def _map_direction_from_request(direction: Any, offset: Any) -> str | None:
        d = str(direction).upper()
        o = str(offset).upper()
        if d in {"BUY", "DIRECTIONBUY"} and o in {"OPEN", "OFFSETOPEN"}:
            return "BUY_OPEN"
        if d in {"BUY", "DIRECTIONBUY"} and o in {"CLOSE", "CLOSETODAY", "CLOSE_TODAY", "OFFSETCLOSE"}:
            return "BUY_CLOSE_TODAY" if o in {"CLOSETODAY", "CLOSE_TODAY"} else "BUY_CLOSE"
        if d in {"SELL", "DIRECTIONSELL"} and o in {"OPEN", "OFFSETOPEN"}:
            return "SELL_OPEN"
        if d in {"SELL", "DIRECTIONSELL"} and o in {"CLOSE", "CLOSETODAY", "CLOSE_TODAY", "OFFSETCLOSE"}:
            return "SELL_CLOSE_TODAY" if o in {"CLOSETODAY", "CLOSE_TODAY"} else "SELL_CLOSE"
        return None
